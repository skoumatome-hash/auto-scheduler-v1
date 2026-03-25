"""GitHub Actions用: スプシから未投稿ストックを取得して投稿

投稿予定時刻（日付付き）が現在以前の未投稿分をまとめて処理。
結果はZ列以降に分割記録（アカウント/日時/ID/URL/ステータス）。
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import gspread
import httpx

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
REPLY_DELAY = int(os.environ.get("REPLY_DELAY", "5"))
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))
GCP_CREDS = json.loads(os.environ.get("GCP_CREDENTIALS", "{}"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

MAX_PER_RUN = 10
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


def _col_letter(col_num):
    """列番号をアルファベットに変換（1=A, 27=AA）"""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def main():
    if not ACCOUNTS:
        print("ACCOUNTS_JSON が設定されていません")
        return

    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)

    all_rows = ws.get_all_records()
    headers = ws.row_values(1)

    # 結果記録用の列を確認/追加
    result_cols = {
        "投稿アカウント": None,
        "投稿日時": None,
        "投稿ID": None,
        "投稿URL": None,
        "ステータス": None,
    }
    for i, h in enumerate(headers):
        if h in result_cols:
            result_cols[h] = i + 1

    # 足りない列を追加
    next_col = len(headers) + 1
    for col_name, col_idx in result_cols.items():
        if col_idx is None:
            result_cols[col_name] = next_col
            ws.update_cell(1, next_col, col_name)
            next_col += 1

    now = datetime.now(JST)
    now_str = now.strftime("%Y-%m-%d %H:%M")
    today_str = now.strftime("%Y-%m-%d")

    # 未投稿数と総数
    total = sum(1 for r in all_rows if r.get("投稿文", ""))
    posted = sum(1 for r in all_rows if r.get("ステータス", "") == "成功" and today_str in str(r.get("投稿日時", "")))
    remaining = total - posted

    print(f"総ストック: {total}件 / 今日投稿済み: {posted}件 / 残り: {remaining}件")
    print(f"現在時刻(JST): {now_str}")

    # 予定時刻を過ぎた未投稿を収集
    targets = []
    for i, row in enumerate(all_rows):
        rewritten = row.get("リライト結果", "")
        scheduled_raw = str(row.get("投稿予定時刻", ""))
        status = str(row.get("ステータス", ""))

        if not rewritten or not scheduled_raw:
            continue
        # 既に成功してたらスキップ
        if status == "成功":
            continue

        # Google Sheetsの時刻ゼロ落ち対策（"2026-03-25 0:30" → "2026-03-25 00:30"）
        try:
            scheduled_dt = datetime.strptime(scheduled_raw, "%Y-%m-%d %H:%M")
            scheduled = scheduled_dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            # 旧形式（時刻のみ "09:00"）の場合は今日の日付を付与
            try:
                scheduled_dt = datetime.strptime(f"{today_str} {scheduled_raw}", "%Y-%m-%d %H:%M")
                scheduled = scheduled_dt.strftime("%Y-%m-%d %H:%M")
            except ValueError:
                continue

        # 予定時刻が未来ならスキップ
        if scheduled > now_str:
            continue

        targets.append((i + 2, row))

    if not targets:
        print("投稿予定のストックなし。スキップ。")
        return

    if len(targets) > MAX_PER_RUN:
        print(f"対象{len(targets)}件 → 今回は{MAX_PER_RUN}件だけ処理")
        targets = targets[:MAX_PER_RUN]

    print(f"今回投稿する件数: {len(targets)}件")

    updates = []
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

            # Z列以降に分割記録
            updates.append({"range": f"{_col_letter(result_cols['投稿アカウント'])}{target_row}", "values": [[f"@{account['name']}"]]})
            updates.append({"range": f"{_col_letter(result_cols['投稿日時'])}{target_row}", "values": [[timestamp]]})
            updates.append({"range": f"{_col_letter(result_cols['投稿ID'])}{target_row}", "values": [[str(post_id)]]})
            updates.append({"range": f"{_col_letter(result_cols['投稿URL'])}{target_row}", "values": [[permalink]]})
            updates.append({"range": f"{_col_letter(result_cols['ステータス'])}{target_row}", "values": [["成功"]]})

            print(f"  成功! post_id={post_id}")
            if permalink:
                print(f"  URL: {permalink}")
            success_count += 1

        except Exception as e:
            error_msg = str(e)[:100]
            timestamp = now.strftime("%Y-%m-%d %H:%M")

            # 失敗も記録
            updates.append({"range": f"{_col_letter(result_cols['投稿アカウント'])}{target_row}", "values": [[f"@{account['name']}"]]})
            updates.append({"range": f"{_col_letter(result_cols['投稿日時'])}{target_row}", "values": [[timestamp]]})
            updates.append({"range": f"{_col_letter(result_cols['ステータス'])}{target_row}", "values": [[f"失敗: {error_msg}"]]})

            print(f"  失敗: {e}")

        # 次の投稿まで30秒wait
        if idx < len(targets) - 1:
            time.sleep(30)

    # バッチ書き込み
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    print(f"\n完了! {success_count}/{len(targets)}件投稿成功")


if __name__ == "__main__":
    main()
