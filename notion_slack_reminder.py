# -*- coding: utf-8 -*-
"""
Notion → Slack Reminder Script (固定メンション + 実施責任者 Select/Multi-select 対応)

仕様:
- 固定メンション: 浅井さん(U09QNJB06DS), ケントさん(U084EL20EV6)
- 「面談リマインド日」が今日のページを抽出（date または formula(date) に対応）
- タイトル列はデフォルト「業務従事者」（必要なら TITLE_PROP を変更）
- 「実施責任者」は People / Select / Multi-select / RichText のいずれでも取得可
- Slack本文には NotionページURL と 実施責任者URL（PERSON_URL_MAP_JSON）を掲載
- Notionの URL プロパティ（既定 "URL"）には最初の実施責任者URLを反映（あれば）

石井寛大　U095FDQE5NF
"""

import os
import re
import json
import time
import logging
import datetime as dt
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import requests
from dotenv import load_dotenv, find_dotenv


# ========= 環境変数ロード（1回だけ） =========
def load_env(dotenv_path: Optional[str] = None, override: bool = True) -> dict:
    candidates: List[Path] = []
    if dotenv_path:
        candidates.append(Path(dotenv_path))
    candidates.append(Path(__file__).resolve().parent / ".env")
    candidates.append(Path.cwd() / ".env")
    fd = find_dotenv(usecwd=True)
    if fd:
        candidates.append(Path(fd))

    loaded: Optional[Path] = None
    for p in candidates:
        if p and p.exists():
            load_dotenv(p, override=override)
            loaded = p
            break
    if not loaded:
        raise FileNotFoundError("`.env` が見つかりません。スクリプト隣接 or カレント直下に配置してください。")

    log_path = os.getenv("LOG_PATH", "reminder.log")
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info(f"Loaded .env from: {loaded}")

    need = ["NOTION_TOKEN", "NOTION_DATABASE_ID", "SLACK_BOT_TOKEN", "SLACK_CHANNEL_ID", "TIMEZONE"]
    env = {k: os.getenv(k) for k in need}
    missing = [k for k, v in env.items() if not v]
    if missing:
        raise EnvironmentError(f"必須環境変数が不足: {', '.join(missing)}")

    # JSON（PERSON_URL_MAP_JSON）
    person_map_raw = os.getenv("PERSON_URL_MAP_JSON", "{}")
    try:
        env["PERSON_URL_MAP"] = json.loads(person_map_raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"PERSON_URL_MAP_JSON が不正なJSONです: {e}")

    # 任意
    env["LOG_PATH"] = log_path
    return env


ENV = load_env()

# ========= 定数 =========
NOTION_TOKEN: str = ENV["NOTION_TOKEN"]
DB_ID: str = ENV["NOTION_DATABASE_ID"]  # 32桁（ハイフン無し）
SLACK_TOKEN: str = ENV["SLACK_BOT_TOKEN"]
DEFAULT_SLACK_CHANNEL: str = ENV["SLACK_CHANNEL_ID"]
TIMEZONE: str = ENV["TIMEZONE"]  # 例: Asia/Tokyo
PERSON_URL_MAP: Dict[str, str] = ENV["PERSON_URL_MAP"]

# Notion列名（必要に応じて変更）
DATE_PROP = "面談リマインド日"
TITLE_PROP = "業務従事者"      # タイトル列（Notion側で title 型の列名）
ASSIGNEE_PROP = "実施責任者"   # Select / Multi-select / People / RichText いずれにも対応

# 固定メンション
MENTION_FIXED = "<@U09QNJB06DS>　<@U084EL20EV6>"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
SLACK_HEADERS = {
    "Authorization": f"Bearer {SLACK_TOKEN}",
    "Content-Type": "application/json; charset=utf-8",
}


# ========= ユーティリティ =========
def normalize_name(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[ \u3000\t\n]", "", s).lower()


def today_iso(tz: str) -> str:
    if tz == "Asia/Tokyo":
        jst = dt.timezone(dt.timedelta(hours=9))
        now = dt.datetime.now(jst)
    else:
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    return now.date().isoformat()


def extract_title(page: dict, title_prop: str = TITLE_PROP) -> str:
    try:
        t = page["properties"][title_prop]["title"]
        return "".join([r.get("plain_text", "") for r in t]).strip() or "(無題)"
    except Exception:
        return "(無題)"


def extract_page_id(page: dict) -> str:
    return page.get("id", "")


def page_url(page: dict) -> str:
    # Notion APIの page object は "url" を持つ
    return page.get("url", "")


def extract_assignees(page: dict, prop_name: str = ASSIGNEE_PROP) -> List[str]:
    """
    Notionの「実施責任者」を型に応じて取り出す:
      - people       → people[].name
      - select       → select.name
      - multi_select → multi_select[].name
      - rich_text    → 分割（、・,/／ 改行）
    """
    try:
        prop = page["properties"][prop_name]
    except KeyError:
        return []

    t = prop.get("type")

    if t == "people":
        ppl = prop.get("people", []) or []
        names = [(p.get("name") or "").strip() for p in ppl]
        return [n for n in names if n]

    if t == "select":
        sel = prop.get("select") or {}
        name = (sel.get("name") or "").strip()
        return [name] if name else []

    if t == "multi_select":
        arr = prop.get("multi_select", []) or []
        names = [(x.get("name") or "").strip() for x in arr]
        return [n for n in names if n]

    if t == "rich_text":
        rt = "".join([r.get("plain_text", "") for r in prop.get("rich_text", [])]).strip()
        if not rt:
            return []
        parts = re.split(r"[、・,/／\n\r]+", rt)
        return [p.strip() for p in parts if p.strip()]

    return []


def resolve_person_urls(names: List[str]) -> List[str]:
    """ENVの PERSON_URL_MAP_JSON で名前一致（スペース差吸収）→ URL収集"""
    urls: List[str] = []
    idx = {normalize_name(k): v for k, v in PERSON_URL_MAP.items() if v}
    for n in names:
        url = idx.get(normalize_name(n))
        if url and url not in urls:
            urls.append(url)
    return urls


def update_notion_url_property(page_id: str, url_value: str, url_prop_name: str = "URL") -> None:
    """Notion の URL プロパティを上書き（存在しない場合は 404 になる場合あり）。"""
    if not url_value:
        return
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": {url_prop_name: {"url": url_value}}}
    res = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    if not res.ok:
        logging.warning(f"update_notion_url_property failed: {res.status_code} {res.text}")


# ========= Notion Query（date / formula(date) 自動対応）=========
def build_date_filter(db_id: str, date_prop: str, yyyy_mm_dd: str) -> dict:
    meta = requests.get(f"https://api.notion.com/v1/databases/{db_id}", headers=NOTION_HEADERS)
    if not meta.ok:
        raise RuntimeError(f"Failed to get DB meta: {meta.status_code} {meta.text}")
    props = meta.json().get("properties", {})
    p = props.get(date_prop)
    if not p:
        raise KeyError(f"Databaseに '{date_prop}' プロパティがありません。")

    t = p.get("type")
    if t == "date":
        return {"filter": {"property": date_prop, "date": {"equals": yyyy_mm_dd}}}
    if t == "formula":
        ftype = p.get("formula", {}).get("type")
        if ftype == "date":
            return {"filter": {"property": date_prop, "formula": {"date": {"equals": yyyy_mm_dd}}}}
        raise TypeError(f"'{date_prop}' は formula({ftype})。date を返していません。")
    raise TypeError(f"'{date_prop}' の型は {t}。date ではありません。")


def notion_query_today(db_id: str, date_prop: str, today: str) -> List[dict]:
    """ページング対応のクエリ（date / formula(date) の型を自動判定）"""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = build_date_filter(db_id, date_prop, today)
    results: List[dict] = []
    while True:
        res = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if not res.ok:
            # デバッグ用に本文も出す
            logging.error(f"Notion query failed: {res.status_code} {res.text}")
            res.raise_for_status()
        data = res.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
    return results


# ========= Slack =========
def slack_post(channel: str, text: str) -> None:
    url = "https://slack.com/api/chat.postMessage"
    payload = {"channel": channel, "text": text}
    res = requests.post(url, headers=SLACK_HEADERS, data=json.dumps(payload))
    ok = res.ok and res.json().get("ok")
    if not ok:
        raise RuntimeError(f"Slack送信エラー: {res.text}")


# ========= Main =========
def main():
    today = today_iso(TIMEZONE)
    logging.info(f"=== Reminder run for {today} ===")

    try:
        pages = notion_query_today(DB_ID, DATE_PROP, today)
        if not pages:
            logging.info("No reminders today.")
            print("No reminders today.")
            return

        for p in pages:
            title = extract_title(p, TITLE_PROP)
            page_id = extract_page_id(p)
            notion_link = page_url(p)

            # 実施責任者（Select / Multi-select / People / RichText 対応）
            names = extract_assignees(p, ASSIGNEE_PROP)
            display_names = "、".join(names) if names else "（実施責任者未設定）"

            # URL解決 & NotionのURL列を更新（先頭のみ反映）
            person_urls = resolve_person_urls(names)
            chosen_url = person_urls[0] if person_urls else ""
            if chosen_url:
                update_notion_url_property(page_id, chosen_url)
                logging.info(f"[{title}] URL updated to {chosen_url}")

            # Slack本文（固定メンションを必ず使用）
            lines: List[str] = [
                f"{MENTION_FIXED}",
                "【リマインド】",
                f"{title}",
                f"Notionページ：{notion_link}",
                f"{display_names}",
            ]
            if person_urls:
                for u in person_urls:
                    lines.append(f"実施責任者URL：{u}")
            else:
                lines.append("実施責任者URL： （未登録）")

            msg = "\n".join(lines)
            slack_post(DEFAULT_SLACK_CHANNEL, msg)
            logging.info(f"[{title}] Slack通知送信済み → 実施責任者:{display_names}")

            time.sleep(1)

        logging.info(f"✅ Completed {len(pages)} reminder(s).")
        print(f"Posted {len(pages)} reminder(s).")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)
        print(f"エラーが発生しました: {e}")
        raise


if __name__ == "__main__":
    main()
