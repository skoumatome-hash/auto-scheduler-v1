"""毎日0時にリライトと一緒に実行: 全垢のimp/フォロワーを取得してスプシに記録"""
import json
import os
from datetime import datetime, timezone, timedelta

import gspread
import httpx

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))
GCP_CREDS = json.loads(os.environ.get("GCP_CREDENTIALS", "{}"))

JST = timezone(timedelta(hours=9))
SHADOW_BAN_THRESHOLD = 10


def threads_api(url):
    """Threads Graph API呼び出し"""
    resp = httpx.get(url, timeout=15)
    return resp.json()


def fetch_all_insights():
    """全垢のインサイトを取得"""
    results = []
    for acc in ACCOUNTS:
        uid = acc["user_id"]
        token = acc["token"]
        name = acc["name"]

        status = "正常稼働"
        followers = 0
        total_views = 0
        posts_data = []

        # ユーザーインサイト（フォロワー数）
        try:
            insights = threads_api(
                f"https://graph.threads.net/v1.0/{uid}/threads_insights"
                f"?metric=views,followers_count&access_token={token}"
            )
            if "error" in insights:
                code = insights["error"].get("code", 0)
                if code in (190, 102):
                    status = "トークン切れ"
                else:
                    status = "エラー"
            else:
                for item in insights.get("data", []):
                    if item["name"] == "followers_count":
                        followers = item.get("total_value", {}).get("value", 0)
                    elif item["name"] == "views":
                        total_views = item.get("total_value", {}).get("value", 0)
        except Exception as e:
            status = f"エラー: {str(e)[:30]}"

        # 直近5投稿のimp取得
        avg_views = 0
        if "エラー" not in status and "トークン" not in status:
            try:
                post_resp = threads_api(
                    f"https://graph.threads.net/v1.0/{uid}/threads"
                    f"?fields=id,text,timestamp,permalink&limit=5&access_token={token}"
                )
                for p in post_resp.get("data", []):
                    pid = p["id"]
                    views = likes = 0
                    try:
                        pi = threads_api(
                            f"https://graph.threads.net/v1.0/{pid}/insights"
                            f"?metric=views,likes&access_token={token}"
                        )
                        for m in pi.get("data", []):
                            if m["name"] == "views":
                                views = m["values"][0]["value"]
                            elif m["name"] == "likes":
                                likes = m["values"][0]["value"]
                    except Exception:
                        pass
                    posts_data.append({"views": views, "likes": likes})

                if posts_data:
                    avg_views = sum(p["views"] for p in posts_data) // len(posts_data)
                    if avg_views < SHADOW_BAN_THRESHOLD and len(posts_data) >= 3:
                        status = "シャドウバン"
            except Exception:
                pass

        total_likes = sum(p["likes"] for p in posts_data)
        total_post_views = sum(p["views"] for p in posts_data)

        results.append({
            "name": name,
            "followers": followers,
            "total_views": total_views,
            "avg_views": avg_views,
            "total_post_views": total_post_views,
            "total_likes": total_likes,
            "status": status,
            "post_count": len(posts_data),
        })

    return results


def save_to_spreadsheet(results):
    """スプシの「インサイトログ」タブに記録"""
    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # インサイトログタブを取得 or 作成
    tab_name = "インサイトログ"
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=10)
        ws.update("A1:J1", [[
            "日付", "アカウント", "フォロワー", "総views(API)",
            "直近5投稿avg views", "直近5投稿合計views", "直近5投稿合計likes",
            "投稿数", "ステータス", "更新時刻"
        ]])

    now = datetime.now(JST)
    today = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    rows = []
    for r in results:
        rows.append([
            today,
            r["name"],
            r["followers"],
            r["total_views"],
            r["avg_views"],
            r["total_post_views"],
            r["total_likes"],
            r["post_count"],
            r["status"],
            time_str,
        ])

    # 既存の当日データがあれば削除してから追記
    existing = ws.get_all_values()
    delete_rows = []
    for i, row in enumerate(existing[1:], start=2):  # ヘッダースキップ
        if row and row[0] == today:
            delete_rows.append(i)

    # 下から削除（インデックスずれ防止）
    for idx in reversed(delete_rows):
        ws.delete_rows(idx)

    # 追記
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    # サマリー表示
    active = sum(1 for r in results if r["status"] == "正常稼働")
    shadow = sum(1 for r in results if r["status"] == "シャドウバン")
    error = sum(1 for r in results if "エラー" in r["status"] or "トークン" in r["status"])
    total_f = sum(r["followers"] for r in results)
    total_v = sum(r["total_post_views"] for r in results)

    print(f"インサイト取得完了: {len(results)}垢")
    print(f"  正常稼働: {active} / シャドウバン: {shadow} / エラー: {error}")
    print(f"  フォロワー合計: {total_f} / 直近投稿views合計: {total_v}")


if __name__ == "__main__":
    print("インサイト取得開始...")
    results = fetch_all_insights()
    save_to_spreadsheet(results)
    print("完了!")
