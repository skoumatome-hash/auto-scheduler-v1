"""GitHub Actions用: スプシから未投稿ストックを1件取得して投稿

24時間でN件のストックを均等間隔で投稿する。
前回投稿からの経過時間をチェックし、間隔に達していたら投稿。
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import anthropic
import gspread
import httpx

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
REPLY_DELAY = int(os.environ.get("REPLY_DELAY", "5"))
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))
GCP_CREDS = json.loads(os.environ.get("GCP_CREDENTIALS", "{}"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# 1日25投稿、58分間隔
POSTS_PER_DAY = 25
CYCLE_HOURS = 24
JST = timezone(timedelta(hours=9))


def api_request(method, url, data=None):
    with httpx.Client(timeout=60) as client:
        if method == "POST":
            resp = client.post(url, data=data)
        else:
            resp = client.get(url)
    body = resp.json()
    if resp.status_code != 200:
        raise Exception(f"API error {resp.status_code}: {json.dumps(body, ensure_ascii=False)}")
    return body


def post_with_reply(account, post_text, reply_text, media_urls):
    user_id = account["user_id"]
    token = account["token"]
    base = f"https://graph.threads.net/v1.0/{user_id}/threads"
    pub = f"https://graph.threads.net/v1.0/{user_id}/threads_publish"

    video_urls = [u for u in media_urls if "/video/upload/" in u or u.endswith((".mp4", ".mov"))]
    image_urls = [u for u in media_urls if u not in video_urls]
    post_media = video_urls if video_urls else image_urls

    p = None
    try:
        if len(post_media) == 0:
            raise ValueError("no media")

        elif len(post_media) == 1:
            is_vid = "/video/upload/" in post_media[0]
            payload = {"media_type": "VIDEO" if is_vid else "IMAGE", "text": post_text, "access_token": token}
            payload["video_url" if is_vid else "image_url"] = post_media[0]
            c = api_request("POST", base, payload)
            time.sleep(5)
            p = api_request("POST", pub, {"creation_id": c["id"], "access_token": token})

        else:
            children = []
            for mu in post_media[:10]:
                is_vid = "/video/upload/" in mu
                payload = {"media_type": "VIDEO" if is_vid else "IMAGE", "is_carousel_item": "true", "access_token": token}
                payload["video_url" if is_vid else "image_url"] = mu
                r = api_request("POST", base, payload)
                children.append(r["id"])
                time.sleep(5)
            car = api_request("POST", base, {
                "media_type": "CAROUSEL", "children": ",".join(children),
                "text": post_text, "access_token": token,
            })
            time.sleep(3)
            p = api_request("POST", pub, {"creation_id": car["id"], "access_token": token})

    except Exception as e:
        # メディア投稿失敗 → テキストのみでフォールバック
        print(f"メディア投稿失敗、テキストのみで投稿: {e}")
        c = api_request("POST", base, {"media_type": "TEXT", "text": post_text, "access_token": token})
        time.sleep(3)
        p = api_request("POST", pub, {"creation_id": c["id"], "access_token": token})

    main_id = p["id"]

    if reply_text:
        time.sleep(REPLY_DELAY)
        rc = api_request("POST", base, {
            "media_type": "TEXT", "text": reply_text,
            "reply_to_id": main_id, "access_token": token,
        })
        time.sleep(3)
        api_request("POST", pub, {"creation_id": rc["id"], "access_token": token})

    return main_id


def rewrite_text(original, level="light"):
    """投稿文をClaude APIでリライト"""
    if not ANTHROPIC_API_KEY:
        return original
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    levels = {
        "light": "言い回しだけ軽く変える。構成・改行はほぼそのまま。元と7割似てOK",
        "medium": "表現や切り口を少しアレンジ。元と5割似てOK",
    }
    resp = client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=1024,
        messages=[{"role": "user", "content": f"""以下の投稿文をリライトしてください。

【元の投稿文】
{original}

【リライト強度】
{levels.get(level, levels['light'])}

【ルール】
- 改行は元投稿を参考に読みやすく
- ハッシュタグは使わない
- 外国語の場合は自然な日本語に翻訳
- リライト結果だけを返して"""}],
    )
    return resp.content[0].text.strip()


def rewrite_reply(original_reply, post_text, amazon_urls, rakuten_urls):
    """リプライをClaude APIでリライト+アフィURL付与"""
    if not ANTHROPIC_API_KEY:
        return original_reply
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=512,
        messages=[{"role": "user", "content": f"""以下のリプライを商品が売れるようにリライトしてください。

【元のメイン投稿】
{post_text[:200]}

【元のリプライ】
{original_reply}

【ルール】
- 紹介文は2〜3行で短く
- 自然な口語体で押し売り感なし
- 外国語なら日本語に翻訳
- URLの下には何も書かない
- 紹介文だけ返して。URL部分はこちらで付ける"""}],
    )
    intro = resp.content[0].text.strip()
    parts = [intro, ""]
    if rakuten_urls:
        parts.append("楽天PR")
        parts.append(rakuten_urls[0])
        parts.append("")
    if amazon_urls:
        parts.append("amazonPR")
        parts.append(amazon_urls[0])
    return "\n".join(parts).strip()


def main():
    if not ACCOUNTS:
        print("ACCOUNTS_JSON が設定されていません")
        return

    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)  # 元データ（ミックス）タブから読んで、投稿前にリライトする

    all_rows = ws.get_all_records()
    headers = ws.row_values(1)

    # 「投稿済み」列を確認/追加
    posted_col = None
    for i, h in enumerate(headers):
        if h == "投稿済み":
            posted_col = i + 1
            break
    if posted_col is None:
        posted_col = len(headers) + 1
        ws.update_cell(1, posted_col, "投稿済み")

    # 未投稿数と総数を確認
    total = sum(1 for r in all_rows if r.get("投稿文", ""))
    posted = sum(1 for r in all_rows if r.get("投稿済み", ""))
    remaining = total - posted

    if remaining == 0:
        print("未投稿のストックなし。終了。")
        return

    # 投稿間隔: 24h ÷ 1日あたりの投稿数（25件）
    daily_posts = min(POSTS_PER_DAY, remaining)
    interval_minutes = (CYCLE_HOURS * 60) / daily_posts
    print(f"総ストック: {total}件 / 投稿済み: {posted}件 / 残り: {remaining}件")
    print(f"1日投稿数: {daily_posts}件 / 間隔: {interval_minutes:.0f}分")

    # 最後の投稿時刻を確認
    last_posted_time = None
    for row in reversed(all_rows):
        posted_val = str(row.get("投稿済み", ""))
        if posted_val:
            parts = posted_val.split(":")
            if len(parts) >= 3:
                try:
                    time_str = ":".join(parts[2:])
                    last_posted_time = datetime.fromisoformat(time_str)
                    break
                except ValueError:
                    pass

    now = datetime.now(JST)

    if last_posted_time:
        elapsed = (now - last_posted_time).total_seconds() / 60
        print(f"前回投稿: {last_posted_time.strftime('%H:%M')} ({elapsed:.0f}分前)")
        if elapsed < interval_minutes:
            wait = interval_minutes - elapsed
            print(f"間隔未達。あと{wait:.0f}分待ち。スキップ。")
            return
    else:
        print("初回投稿")

    # 未投稿ストックを探す
    target_row = None
    target_data = None
    for i, row in enumerate(all_rows):
        if not row.get("投稿済み", "") and row.get("投稿文", ""):
            target_row = i + 2
            target_data = row
            break

    if not target_data:
        print("未投稿のストックなし。")
        return

    # アカウント選択（ラウンドロビン、同じ日に同じ垢を使い回さない）
    today_str = now.strftime("%Y-%m-%d")
    used_today = set()
    for row in all_rows:
        pv = str(row.get("投稿済み", ""))
        if today_str in pv:
            acc_name = pv.split(":")[0]
            used_today.add(acc_name)

    account = None
    for acc in ACCOUNTS:
        if acc["name"] not in used_today:
            account = acc
            break
    if not account:
        # 全垢使い切った場合はラウンドロビン
        account = ACCOUNTS[(target_row - 2) % len(ACCOUNTS)]

    # 投稿文をリライト
    original_text = target_data.get("投稿文", "")
    post_text = rewrite_text(original_text)
    print(f"リライト完了: {post_text[:60]}...")

    # リプライ（URLがある場合のみ）
    amazon_url = target_data.get("amazonURL", "")
    rakuten_url = target_data.get("楽天URL", "")
    amazon_list = amazon_url.split() if amazon_url else []
    rakuten_list = rakuten_url.split() if rakuten_url else []

    reply_text = ""
    if amazon_list or rakuten_list:
        original_reply = target_data.get("リプライ文言", "")
        reply_text = rewrite_reply(original_reply, original_text, amazon_list, rakuten_list)
        print(f"リプライ: {reply_text[:60]}...")

    # メディア
    media = []
    for key in ["素材URL1","素材URL2","素材URL3","素材URL4","素材URL5","素材URL6","素材URL7","素材URL8","素材URL9","素材URL10"]:
        v = target_data.get(key, "")
        if v:
            media.append(v)

    print(f"投稿: row={target_row} account=@{account['name']}")
    print(f"メディア: {len(media)}件")

    post_id = post_with_reply(account, post_text, reply_text, media)

    # permalink取得
    permalink = ""
    try:
        info = api_request("GET", f"https://graph.threads.net/v1.0/{post_id}?fields=permalink&access_token={account['token']}")
        permalink = info.get("permalink", "")
    except Exception:
        pass

    # 投稿済みフラグ（垢名 | 日時 | post_id | permalink）
    timestamp = now.strftime("%Y-%m-%d %H:%M")
    posted_value = f"@{account['name']} | {timestamp} | {post_id} | {permalink}"
    ws.update_cell(target_row, posted_col, posted_value)
    print(f"成功! @{account['name']} post_id={post_id} at {now.strftime('%H:%M')}")
    if permalink:
        print(f"URL: {permalink}")


if __name__ == "__main__":
    main()
