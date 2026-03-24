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

    now = datetime.now(JST)
    now_hm = now.strftime("%H:%M")
    today_str = now.strftime("%Y-%m-%d")

    print(f"総ストック: {total}件 / 投稿済み: {posted}件 / 残り: {remaining}件")
    print(f"現在時刻(JST): {now_hm}")

    # 予定時刻を過ぎた未投稿をまとめて収集
    targets = []
    for i, row in enumerate(all_rows):
        rewritten = row.get("リライト結果", "")
        scheduled = row.get("投稿予定時刻", "")
        posted_val = str(row.get("投稿済み", ""))

        if not rewritten or not scheduled:
            continue
        if today_str in posted_val:
            continue
        if scheduled > now_hm:
            continue

        targets.append((i + 2, row))

    if not targets:
        print("投稿予定のストックなし。スキップ。")
        return

    print(f"今回投稿する件数: {len(targets)}件")

    # まとめて投稿（各投稿間に30秒wait）
    success_count = 0
    for idx, (target_row, target_data) in enumerate(targets):
        # 担当垢
        assigned = target_data.get("担当垢", "").replace("@", "")
        account = None
        for acc in ACCOUNTS:
            if acc["name"] == assigned:
                account = acc
                break
        if not account:
            account = ACCOUNTS[(target_row - 2) % len(ACCOUNTS)]

        post_text = target_data.get("リライト結果", "")
        reply_text = target_data.get("リプライ結果", "")

        media = []
        for key in ["素材URL1","素材URL2","素材URL3","素材URL4","素材URL5","素材URL6","素材URL7","素材URL8","素材URL9","素材URL10"]:
            v = target_data.get(key, "")
            if v:
                media.append(v)

        print(f"\n[{idx+1}/{len(targets)}] row={target_row} @{account['name']} 予定:{target_data.get('投稿予定時刻','')}")

        try:
            post_id = post_with_reply(account, post_text, reply_text, media)

            permalink = ""
            try:
                info = api_request("GET", f"https://graph.threads.net/v1.0/{post_id}?fields=permalink&access_token={account['token']}")
                permalink = info.get("permalink", "")
            except Exception:
                pass

            timestamp = now.strftime("%Y-%m-%d %H:%M")
            posted_value = f"@{account['name']} | {timestamp} | {post_id} | {permalink}"
            ws.update_cell(target_row, posted_col, posted_value)
            print(f"  成功! post_id={post_id}")
            if permalink:
                print(f"  URL: {permalink}")
            success_count += 1

        except Exception as e:
            print(f"  失敗: {e}")

        # 次の投稿まで30秒wait（最後の投稿以外）
        if idx < len(targets) - 1:
            time.sleep(30)

    print(f"\n完了! {success_count}/{len(targets)}件投稿成功")


if __name__ == "__main__":
    main()
