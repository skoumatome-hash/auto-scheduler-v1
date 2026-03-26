"""毎日0時にリライトと一緒に実行: 全垢のimp/フォロワーを取得してスプシに記録"""
import json
import os
from datetime import datetime, timezone, timedelta

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


def save_to_json(results):
    """JSONファイルとして保存（Gitにコミットしてダッシュボードが読む）"""
    now = datetime.now(JST)
    today = now.strftime("%Y-%m-%d")

    # 日別JSONファイルに出力
    output = {
        "date": today,
        "updated_at": now.strftime("%Y-%m-%d %H:%M"),
        "accounts": results,
    }

    # data/ディレクトリに保存
    os.makedirs("data", exist_ok=True)
    filepath = f"data/insights_{today}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 最新データとしても保存
    with open("data/insights_latest.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # サマリー表示
    active = sum(1 for r in results if r["status"] == "正常稼働")
    shadow = sum(1 for r in results if r["status"] == "シャドウバン")
    error = sum(1 for r in results if "エラー" in r["status"] or "トークン" in r["status"])
    total_f = sum(r["followers"] for r in results)
    total_v = sum(r["total_post_views"] for r in results)

    print(f"インサイト取得完了: {len(results)}垢 → {filepath}")
    print(f"  正常稼働: {active} / シャドウバン: {shadow} / エラー: {error}")
    print(f"  フォロワー合計: {total_f} / 直近投稿views合計: {total_v}")


if __name__ == "__main__":
    print("インサイト取得開始...")
    results = fetch_all_insights()
    save_to_json(results)
    print("完了!")
