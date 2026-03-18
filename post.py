"""GitHub Actions用: スプシから未投稿ストックを1件取得して投稿"""
import json
import os
import time

import gspread
import httpx

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
REPLY_DELAY = int(os.environ.get("REPLY_DELAY", "5"))
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))
GCP_CREDS = json.loads(os.environ.get("GCP_CREDENTIALS", "{}"))


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

    # メイン投稿
    if len(post_media) == 0:
        c = api_request("POST", base, {"media_type": "TEXT", "text": post_text, "access_token": token})
        time.sleep(3)
        p = api_request("POST", pub, {"creation_id": c["id"], "access_token": token})

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

    main_id = p["id"]

    # リプライ
    if reply_text:
        time.sleep(REPLY_DELAY)
        rc = api_request("POST", base, {
            "media_type": "TEXT", "text": reply_text,
            "reply_to_id": main_id, "access_token": token,
        })
        time.sleep(3)
        api_request("POST", pub, {"creation_id": rc["id"], "access_token": token})

    return main_id


def main():
    if not ACCOUNTS:
        print("ACCOUNTS_JSON が設定されていません")
        return

    # スプシ接続
    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(1)  # シート2

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

    # 未投稿ストックを探す
    target_row = None
    target_data = None
    for i, row in enumerate(all_rows):
        if not row.get("投稿済み", "") and row.get("投稿文", ""):
            target_row = i + 2
            target_data = row
            break

    if not target_data:
        print("未投稿のストックなし。終了。")
        return

    # アカウント選択（ラウンドロビン）
    account = ACCOUNTS[(target_row - 2) % len(ACCOUNTS)]

    # 投稿データ
    post_text = target_data.get("投稿文", "")
    reply_text = target_data.get("リプライ", "")
    media = [target_data.get(f"素材{i}", "") for i in range(1, 11) if target_data.get(f"素材{i}", "")]

    print(f"投稿: row={target_row} account=@{account['name']}")
    print(f"テキスト: {post_text[:60]}...")
    print(f"メディア: {len(media)}件")

    # 投稿
    post_id = post_with_reply(account, post_text, reply_text, media)

    # 投稿済みフラグ更新
    ws.update_cell(target_row, posted_col, f"{account['name']}:{post_id}")
    print(f"成功! @{account['name']} post_id={post_id}")


if __name__ == "__main__":
    main()
