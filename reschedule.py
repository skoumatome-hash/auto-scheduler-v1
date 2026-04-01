"""毎日0時に実行: スケジュール再割り振り+投稿済みリセット（リライトなし）
Claude API不要。土日・連休中も自動で投稿を回し続ける。
"""
import os
import json
import random
import re
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials

JST = timezone(timedelta(hours=9))

# 環境変数
ACCOUNTS_JSON = os.environ.get("ACCOUNTS_JSON", "[]")
GCP_CREDENTIALS = os.environ.get("GCP_CREDENTIALS", "{}")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")

ACCOUNTS = json.loads(ACCOUNTS_JSON)


def _col_letter(idx):
    """0始まりのカラムインデックスをA, B, ..., Z, AA, AB...に変換"""
    if idx < 26:
        return chr(65 + idx)
    return chr(64 + idx // 26) + chr(65 + idx % 26)


def main():
    now = datetime.now(JST)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # スプシ接続
    creds_json = json.loads(GCP_CREDENTIALS)
    creds = Credentials.from_service_account_info(
        creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("元データ（美容）")

    all_rows = ws.get_all_records()
    header = ws.row_values(1)

    # カラム位置を特定
    needed_cols = {}
    for col_name in ["投稿済み", "担当垢", "投稿予定時刻", "投稿アカウント", "投稿日時", "投稿ID", "投稿URL", "ステータス"]:
        if col_name in header:
            needed_cols[col_name] = header.index(col_name)

    # 投稿対象の行を収集（投稿文がある行）
    post_rows = []
    for idx, row in enumerate(all_rows):
        row_num = idx + 2
        text = str(row.get("投稿文", "") or row.get("リライト結果", ""))
        if not text.strip():
            continue
        post_rows.append((row_num, row))

    total = len(post_rows)
    if total == 0:
        print("投稿対象なし")
        return

    # シャッフル
    random.shuffle(post_rows)

    # 投稿間隔
    interval_minutes = (24 * 60) // total
    print(f"総ストック: {total}件")
    print(f"稼働垢: {len(ACCOUNTS)}垢")
    print(f"投稿間隔: {interval_minutes}分")

    # スケジュール割り振り
    updates = []
    for idx, (row_num, row) in enumerate(post_rows):
        account = ACCOUNTS[idx % len(ACCOUNTS)]
        scheduled_time = today_start + timedelta(minutes=interval_minutes * idx)
        time_str = scheduled_time.strftime("%Y-%m-%d %H:%M")

        # 担当垢+投稿予定時刻
        if "担当垢" in needed_cols:
            updates.append({
                "range": f"{_col_letter(needed_cols['担当垢'])}{row_num}",
                "values": [[account["name"]]],
            })
        if "投稿予定時刻" in needed_cols:
            updates.append({
                "range": f"{_col_letter(needed_cols['投稿予定時刻'])}{row_num}",
                "values": [[time_str]],
            })

        # 投稿済みフラグをクリア
        for col_name in ["投稿済み", "投稿アカウント", "投稿日時", "投稿ID", "投稿URL", "ステータス"]:
            if col_name in needed_cols:
                updates.append({
                    "range": f"{_col_letter(needed_cols[col_name])}{row_num}",
                    "values": [[""]],
                })

    # バッチ更新
    for start in range(0, len(updates), 500):
        ws.batch_update(updates[start:start + 500], value_input_option="USER_ENTERED")

    print(f"スケジュール再割り振り完了: {total}件")


if __name__ == "__main__":
    main()
