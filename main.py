"""Cloud Function: Threads自動投稿

Cloud Schedulerから定期呼び出しされ、スプシから未投稿のストックを1件取得して投稿する。
5垢にラウンドロビンで割り振り。
"""
import json
import os
import time
import re

import functions_framework
import gspread
import httpx

# 環境変数から設定
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
REPLY_DELAY = int(os.environ.get("REPLY_DELAY", "5"))

# 5垢の情報
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))


def _api_request(method, url, data=None):
    """Threads API リクエスト"""
    with httpx.Client(timeout=60) as client:
        if method == "POST":
            resp = client.post(url, data=data)
        else:
            resp = client.get(url)
    body = resp.json()
    if resp.status_code != 200:
        raise Exception(f"API error {resp.status_code}: {json.dumps(body)}")
    return body


def post_with_reply(account, post_text, reply_text, media_urls):
    """メイン投稿 + リプライ"""
    user_id = account["user_id"]
    token = account["token"]
    url_base = f"https://graph.threads.net/v1.0/{user_id}/threads"
    pub_url = f"https://graph.threads.net/v1.0/{user_id}/threads_publish"

    video_urls = [u for u in media_urls if "/video/upload/" in u or u.endswith((".mp4", ".mov"))]
    image_urls = [u for u in media_urls if u not in video_urls]
    post_media = video_urls if video_urls else image_urls

    # メイン投稿
    if len(post_media) == 0:
        container = _api_request("POST", url_base, data={
            "media_type": "TEXT", "text": post_text, "access_token": token,
        })
        time.sleep(3)
        pub = _api_request("POST", pub_url, data={"creation_id": container["id"], "access_token": token})

    elif len(post_media) == 1:
        is_video = "/video/upload/" in post_media[0]
        payload = {"media_type": "VIDEO" if is_video else "IMAGE", "text": post_text, "access_token": token}
        payload["video_url" if is_video else "image_url"] = post_media[0]
        container = _api_request("POST", url_base, data=payload)
        time.sleep(5)
        pub = _api_request("POST", pub_url, data={"creation_id": container["id"], "access_token": token})

    else:
        children_ids = []
        for media_url in post_media[:10]:
            is_video = "/video/upload/" in media_url
            payload = {"media_type": "VIDEO" if is_video else "IMAGE", "is_carousel_item": "true", "access_token": token}
            payload["video_url" if is_video else "image_url"] = media_url
            result = _api_request("POST", url_base, data=payload)
            children_ids.append(result["id"])
            time.sleep(5)
        carousel = _api_request("POST", url_base, data={
            "media_type": "CAROUSEL", "children": ",".join(children_ids),
            "text": post_text, "access_token": token,
        })
        time.sleep(3)
        pub = _api_request("POST", pub_url, data={"creation_id": carousel["id"], "access_token": token})

    main_post_id = pub["id"]

    # リプライ
    if reply_text:
        time.sleep(REPLY_DELAY)
        reply_container = _api_request("POST", url_base, data={
            "media_type": "TEXT", "text": reply_text,
            "reply_to_id": main_post_id, "access_token": token,
        })
        time.sleep(3)
        _api_request("POST", pub_url, data={"creation_id": reply_container["id"], "access_token": token})

    return main_post_id


@functions_framework.http
def scheduled_post(request):
    """Cloud Schedulerから呼ばれるエントリポイント"""
    try:
        # サービスアカウントでスプシ接続
        gc = gspread.service_account_from_dict(json.loads(os.environ.get("GCP_CREDENTIALS", "{}")))
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.get_worksheet(1)  # シート2

        all_rows = ws.get_all_records()

        # 「投稿済み」列を確認（なければ追加）
        headers = ws.row_values(1)
        posted_col_idx = None
        for i, h in enumerate(headers):
            if h == "投稿済み":
                posted_col_idx = i + 1
                break

        if posted_col_idx is None:
            # 投稿済み列を追加
            next_col = len(headers) + 1
            ws.update_cell(1, next_col, "投稿済み")
            posted_col_idx = next_col

        # 未投稿のストックを探す
        target_row = None
        target_data = None
        for i, row in enumerate(all_rows):
            posted = row.get("投稿済み", "")
            if not posted and row.get("投稿文", ""):
                target_row = i + 2  # ヘッダー分+1、0始まり+1
                target_data = row
                break

        if not target_data:
            return json.dumps({"status": "no_stock", "message": "未投稿のストックがありません"}), 200

        # アカウント選択（ラウンドロビン: 行番号 % アカウント数）
        account_idx = (target_row - 2) % len(ACCOUNTS)
        account = ACCOUNTS[account_idx]

        # 投稿データ取得
        post_text = target_data.get("投稿文", "")
        reply_text = target_data.get("リプライ", "")
        media = []
        for i in range(1, 11):
            url = target_data.get(f"素材{i}", "")
            if url:
                media.append(url)

        # 投稿実行
        post_id = post_with_reply(account, post_text, reply_text, media)

        # 投稿済みフラグを更新
        ws.update_cell(target_row, posted_col_idx, f"{account['name']}:{post_id}")

        return json.dumps({
            "status": "success",
            "account": account["name"],
            "post_id": post_id,
            "row": target_row,
        }), 200

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}), 500
