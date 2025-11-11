import os
import json
import time
import datetime as dt
from typing import List, Dict, Tuple, Optional
import logging

import requests
from dotenv import load_dotenv
# --- ログ設定 ---
LOG_PATH = os.getenv("LOG_PATH", "reminder.log")
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DATABASE_ID")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DEFAULT_SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")
TZ = os.getenv("TIMEZONE", "Asia/Tokyo")
PERSON_URL_MAP = json.loads(os.getenv("PERSON_URL_MAP_JSON", "{}"))  # {"氏名": "URL", ...}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
SLACK_HEADERS = {
    "Authorization": f"Bearer {SLACK_TOKEN}",
    "Content-Type": "application/json; charset=utf-8",
}

# ---------- 共通ユーティリティ ----------
def normalize_name(s: str) -> str:
    """全角/半角スペース、タブ、改行を除去して小文字化。"""
    if not s:
        return ""
    # 全角スペース(U+3000) を半角に寄せずとも、丸ごと削除でOK
    return (
        s.replace(" ", "")
         .replace("\u3000", "")
         .replace("\t", "")
         .replace("\n", "")
         .lower()
    )

def today_iso(tz: str) -> str:
    jst = dt.timezone(dt.timedelta(hours=9))
    now = dt.datetime.now(jst if tz == "Asia/Tokyo" else dt.timezone.utc)
    return now.date().isoformat()

# ---------- Notion ----------
def notion_query_today(db_id: str, date_prop: str, today: str) -> List[dict]:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"filter": {"property": date_prop, "date": {"equals": today}}}
    results = []
    while True:
        res = requests.post(url, headers=NOTION_HEADERS, json=payload)
        res.raise_for_status()
        data = res.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return results

def extract_title(page: dict, title_prop="名前") -> str:
    try:
        t = page["properties"][title_prop]["title"]
        return "".join([r["plain_text"] for r in t]) or "(無題)"
    except KeyError:
        return "(無題)"

def extract_page_id(page: dict) -> str:
    return page.get("id", "")

def extract_people(page: dict, prop="実施責任者") -> List[Tuple[str, str]]:
    """[(name, email or "")]"""
    out = []
    try:
        people = page["properties"][prop]["people"]
        for p in people:
            name = p.get("name") or ""
            person = p.get("person") or {}
            email = person.get("email") or ""
            out.append((name, email))
    except KeyError:
        pass
    return out

def extract_slackid_fallback(page: dict, prop="SlackID") -> List[str]:
    ids = []
    try:
        for r in page["properties"][prop]["rich_text"]:
            text = (r.get("plain_text") or "").strip()
            if text.startswith("U") and len(text) >= 8:
                ids.append(text)
    except KeyError:
        pass
    return ids

def update_notion_url_property(page_id: str, url_value: str, url_prop_name: str = "URL") -> None:
    """Notion の URL プロパティを上書き。存在しない場合は無視される可能性あり。"""
    if not url_value:
        return
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": {url_prop_name: {"url": url_value}}}
    res = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    # 404（プロパティなし）などは警告にとどめる
    if not res.ok:
        print(f"[WARN] update_notion_url_property failed: {res.status_code} {res.text}")

def page_url(page: dict) -> str:
    return page.get("url", "")

# ---------- Slack ----------
def slack_post(channel: str, text: str, blocks: Optional[List[dict]] = None):
    url = "https://slack.com/api/chat.postMessage"
    payload = {"channel": channel, "text": text}
    if blocks:
        payload["blocks"] = blocks
    res = requests.post(url, headers=SLACK_HEADERS, data=json.dumps(payload))
    if not res.ok or not res.json().get("ok"):
        raise RuntimeError(f"Slack post error: {res.text}")

def slack_lookup_user_id_by_email(email: str) -> Optional[str]:
    if not email:
        return None
    url = "https://slack.com/api/users.lookupByEmail"
    res = requests.get(url, headers={"Authorization": f"Bearer {SLACK_TOKEN}"}, params={"email": email})
    data = res.json()
    if data.get("ok"):
        return data["user"]["id"]
    return None

def slack_fetch_user_directory() -> List[dict]:
    """全ユーザーを users.list ページングで取得（名前検索用）"""
    url = "https://slack.com/api/users.list"
    users = []
    cursor = None
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        res = requests.get(url, headers={"Authorization": f"Bearer {SLACK_TOKEN}"}, params=params)
        data = res.json()
        if not data.get("ok"):
            raise RuntimeError(f"users.list failed: {data}")
        users.extend(data.get("members", []))
        cursor = data.get("response_metadata", {}).get("next_cursor") or ""
        if not cursor:
            break
    return users

def build_name_index(users: List[dict]) -> Dict[str, str]:
    """
    正規化した氏名 -> SlackユーザーID
    real_name と display_name の両方で索引化。後勝ち回避で先勝ち優先。
    """
    idx: Dict[str, str] = {}
    for u in users:
        uid = u.get("id")
        prof = u.get("profile", {}) or {}
        names = [
            prof.get("real_name", ""),
            prof.get("display_name", ""),
        ]
        for n in names:
            key = normalize_name(n)
            if key and key not in idx:
                idx[key] = uid
    return idx

# ---------- メンション解決（名前優先） ----------
def build_mentions(page: dict, name_index: Dict[str, str]) -> Tuple[str, List[str], List[str]]:
    """
    return:
      display_names: 表示用 "A、B"
      mention_ids:   Slack <@uid> 用 ID 群
      matched_personal_urls: 対象者URL（PERSON_URL_MAPに該当した人のURL）複数可
    優先順位:
      1) 名前→Slack ID（スペース/大小無視）
      2) email→Slack ID（users.lookupByEmail）
      3) SlackID列（Uxxxx）
    """
    assigned = extract_people(page, "実施責任者")
    fallback_ids = extract_slackid_fallback(page)

    display_names: List[str] = []
    mention_ids: List[str] = []
    seen = set()
    matched_urls: List[str] = []

    # 1) 名前優先
    for name, email in assigned:
        display_names.append(name or email or "（担当者未設定）")
        key = normalize_name(name)
        uid = name_index.get(key)
        if uid and uid not in seen:
            mention_ids.append(uid)
            seen.add(uid)

        # 対象者URLマッチ（スペース差吸収で判定）
        if name:
            for target_name, url in PERSON_URL_MAP.items():
                if normalize_name(target_name) == key and url and url not in matched_urls:
                    matched_urls.append(url)

    # 2) メールで補完
    for name, email in assigned:
        if email:
            uid = slack_lookup_user_id_by_email(email)
            if uid and uid not in seen:
                mention_ids.append(uid)
                seen.add(uid)

    # 3) SlackID列
    for uid in fallback_ids:
        if uid not in seen:
            mention_ids.append(uid)
            seen.add(uid)

    return ("、".join([n for n in display_names if n]) or "（担当者未設定）"), mention_ids, matched_urls

# ---------- メイン ----------
def main():
    today = today_iso(TZ)
    pages = notion_query_today(DB_ID, "リマインド日", today)
    if not pages:
        print("No reminders today.")
        return

    # Slackユーザー名インデックス（名前→UID）
    user_dir = slack_fetch_user_directory()
    name_index = build_name_index(user_dir)

    for p in pages:
        title = extract_title(p, "名前")
        page_id = extract_page_id(p)
        display_names, ids, personal_urls = build_mentions(p, name_index)
        notion_link = page_url(p)

        # 対象者URLが見つかったら NotionのURL列を更新（複数あれば最初を採用）
        chosen_url = personal_urls[0] if personal_urls else ""
        if chosen_url:
            update_notion_url_property(page_id, chosen_url)

        # メンション文
        mention_text = " ".join([f"<@{uid}>" for uid in ids]) if ids else "<!channel>"

        # Slack本文（対象者URLがあれば載せる）
        extra_lines = []
        if chosen_url:
            extra_lines.append(f"・担当者URL：{chosen_url}")
        elif personal_urls:
            # 複数あった時の参考表示（Notionには先頭のみ反映）
            extra_lines.append("・担当者URL候補：\n" + "\n".join([f"  - {u}" for u in personal_urls]))

        msg = (
            f"⏰ *本日のリマインド*\n"
            f"・件名：*{title}*\n"
            f"・担当：{display_names}\n"
            f"・Notion：{notion_link}\n"
            + ("\n".join(extra_lines) + "\n" if extra_lines else "")
            + f"\n{mention_text} 対応お願いします。"
        )

        slack_post(DEFAULT_SLACK_CHANNEL, msg)
        time.sleep(1)

    print(f"Posted {len(pages)} reminder(s).")
    
        
    today = today_iso(TZ)
    logging.info(f"=== Reminder run for {today} ===")

    try:
        pages = notion_query_today(DB_ID, "リマインド日", today)
        if not pages:
            logging.info("No reminders today.")
            return

        user_dir = slack_fetch_user_directory()
        name_index = build_name_index(user_dir)

        for p in pages:
            title = extract_title(p, "名前")
            page_id = extract_page_id(p)
            display_names, ids, personal_urls = build_mentions(p, name_index)
            notion_link = page_url(p)

            chosen_url = personal_urls[0] if personal_urls else ""
            if chosen_url:
                update_notion_url_property(page_id, chosen_url)
                logging.info(f"[{title}] URL updated to {chosen_url}")

            mention_text = " ".join([f"<@{uid}>" for uid in ids]) if ids else "<!channel>"

            msg = (
                f"⏰ *本日のリマインド*\n"
                f"・件名：*{title}*\n"
                f"・担当：{display_names}\n"
                f"・Notion：{notion_link}\n"
                + (f"・担当者URL：{chosen_url}\n" if chosen_url else "")
                + f"\n{mention_text} 対応お願いします。"
            )

            slack_post(DEFAULT_SLACK_CHANNEL, msg)
            logging.info(f"[{title}] Slack通知送信済み → {display_names}")

            time.sleep(1)

        logging.info(f"✅ Completed {len(pages)} reminder(s).")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)
        raise e


if __name__ == "__main__":
    main()
