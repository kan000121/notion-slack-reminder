# -*- coding: utf-8 -*-
"""
Notion → Slack Reminder Script
仕様:
- 固定メンション: 浅井さん(U09QNJB06DS), ケントさん(U084EL20EV6)
- 実施責任者 (rich_text) と URL (url) を参照


石井寛大　U095FDQE5NF
"""
# -*- coding: utf-8 -*-
"""
Notion → Slack Reminder Script (fixed mentions + dual URLs)
- 固定メンション: 浅井さん(U09QNJB06DS), ケントさん(U084EL20EV6)
- 実施責任者 (rich_text) から名前を抽出（スペース差吸収）→ PERSON_URL_MAP_JSON で個人URL解決
- Slackには「NotionページURL」と「担当者URL(1人以上なら複数行)」を両方掲載
"""


import os
import json
import time
import datetime as dt
from typing import List, Dict, Tuple, Optional
# --- ログ設定 ---
LOG_PATH = os.getenv("LOG_PATH", "reminder.log")

import re
import requests
import logging
from dotenv import load_dotenv

# --- ENV ---
load_dotenv()
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DATABASE_ID")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tokyo")
LOG_PATH = os.getenv("LOG_PATH", "reminder.log")
PERSON_URL_MAP = json.loads(os.getenv("PERSON_URL_MAP_JSON", "{}"))  # 例: {"石井寛大":"https://example.com/k-ishii","角田隆司":"..."}

# --- LOG ---

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


MENTION_FIXED = "<@U09QNJB06DS>　<@U084EL20EV6>"  # 浅井さん・ケントさん

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

def notion_query_today(db_id: str, date_prop: str, today: str):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"filter": {"property": date_prop, "date": {"equals": today}}}
    res = requests.post(url, headers=NOTION_HEADERS, json=payload)
    res.raise_for_status()
    return res.json().get("results", [])

def extract_title(page: dict, title_prop="業務従事者") -> str:
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
    pages = notion_query_today(DB_ID, "面談リマインド日", today)
    if not pages:
        print("No reminders today.")
        return

    # Slackユーザー名インデックス（名前→UID）
    user_dir = slack_fetch_user_directory()
    name_index = build_name_index(user_dir)

    for p in pages:
        title = extract_title(p, "業務実施者")
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

def extract_richtext(page: dict, prop: str) -> str:
    try:
        t = page["properties"][prop]["rich_text"]
        return "".join([r["plain_text"] for r in t]).strip() or "（未設定）"
    except KeyError:
        return "（未設定）"
    
def extract_assignees(page: dict, prop_name: str = "実施責任者") -> list[str]:
    """
    Notionの「実施責任者」を People 優先で取得。
    無ければ rich_text を分割して取得。
    """
    try:
        prop = page["properties"][prop_name]
    except KeyError:
        return []

    t = prop.get("type")
    if t == "people":
        ppl = prop.get("people", [])
        names = [p.get("name", "").strip() for p in ppl if p.get("name")]
        return [n for n in names if n]
    elif t == "rich_text":
        rt = "".join([r.get("plain_text", "") for r in prop.get("rich_text", [])]).strip()
        if not rt:
            return []
        # 区切り文字：読点・中点・スラッシュ・改行など
        parts = re.split(r"[、・,/／\n\r]+", rt)
        return [p.strip() for p in parts if p.strip()]
    else:
        return []

def build_display_and_urls(page: dict) -> tuple[str, list[str]]:
    """表示用の『実施責任者』文字列と、PERSON_URL_MAP から解決したURL一覧を返す"""
    names = extract_assignees(page, "実施責任者")
    display = "、".join(names) if names else "（未設定）"
    urls = resolve_person_urls(names)  # 既存の関数（ENVの PERSON_URL_MAP_JSON を使う）
    return display, urls

def page_url(page: dict) -> str:
    return page.get("url", "（Notion URL 取得不可）")

def slack_post(channel: str, text: str):
    url = "https://slack.com/api/chat.postMessage"
    payload = {"channel": channel, "text": text}
    res = requests.post(url, headers=SLACK_HEADERS, data=json.dumps(payload))
    if not res.ok or not res.json().get("ok"):
        raise RuntimeError(f"Slack送信エラー: {res.text}")

def normalize_name(s: str) -> str:
    if not s: return ""
    return re.sub(r"[ \u3000\t\n]", "", s).lower()  # 半角/全角スペース等を除去し小文字化

def split_names(s: str):
    """実施責任者のrich_textを '、' '・' '/' '／' などで分割。空は除外。"""
    if not s or s == "（未設定）":
        return []
    parts = re.split(r"[、・,/／\n\r]+", s)
    return [p.strip() for p in parts if p.strip()]

def resolve_person_urls(names: list[str]) -> list[str]:
    """ENVのPERSON_URL_MAP_JSONで名前一致（スペース差吸収）→ URL収集"""
    urls = []
    # 逆引き用インデックス（正規化名→URL）
    idx = {normalize_name(k): v for k, v in PERSON_URL_MAP.items() if v}
    for n in names:
        url = idx.get(normalize_name(n))
        if url and url not in urls:
            urls.append(url)
    return urls

# --- Main ---
def main():
    today = today_iso(TIMEZONE)

    logging.info(f"=== Reminder run for {today} ===")

    try:
        pages = notion_query_today(DB_ID, "面談リマインド日", today)
        if not pages:
            logging.info("No reminders today.")

            return

        user_dir = slack_fetch_user_directory()
        name_index = build_name_index(user_dir)

        for p in pages:
            title = extract_title(p, "業務従事者")
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
                f"・業務従事者：*{title}*\n"
                f"・担当：{display_names}\n"
                f"・Notion：{notion_link}\n"
                + (f"・担当者URL：{chosen_url}\n" if chosen_url else "")
                + f"\n{mention_text} 対応お願いします。"
            )

            slack_post(DEFAULT_SLACK_CHANNEL, msg)
            logging.info(f"[{title}] Slack通知送信済み → {display_names}")

            time.sleep(1)


            print("本日のリマインド対象はありません。")
            return

        for p in pages:
            title_candidate = extract_richtext(p, "業務従事者")
            title = title_candidate if title_candidate not in ("", "（未設定）") else (extract_title(p, "業務従事者") or "（無題）")
            notion_link = page_url(p)

            # ← ここが肝：People/テキスト両対応
            jisseki_display, person_urls = build_display_and_urls(p)

            # 固定メンション・・
            mention_text = "<@U09QNJB06DS>　<@U084EL20EV6>"

            # 文章：NotionページURL ＋ 担当者URL（複数なら全部）
            lines = [
                f"{mention_text}",
                "【リマインド】",
                f"{title}",
                f"Notionページ：{notion_link}",
                f"{jisseki_display}",
            ]
            if person_urls:
                for u in person_urls:
                    lines.append(f"担当者URL：{u}")
            else:
                lines.append("担当者URL：「（未登録）」")

            msg = "\n".join(lines)

            slack_post(SLACK_CHANNEL, msg)
            logging.info(f"[{title}] 通知完了 → 実施責任者:{jisseki_display} / URLs:{person_urls or 'なし'}")
            time.sleep(1)



        logging.info(f"✅ Completed {len(pages)} reminder(s).")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)
        raise e


        print(f"エラーが発生しました: {e}")
if __name__ == "__main__":
    main()
