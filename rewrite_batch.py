"""毎日0時に実行: 143投稿全部をリライト+担当垢+投稿予定時刻を元データタブに書く"""
import json
import os
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import anthropic
import gspread
import httpx

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
ACCOUNTS = json.loads(os.environ.get("ACCOUNTS_JSON", "[]"))
GCP_CREDS = json.loads(os.environ.get("GCP_CREDENTIALS", "{}"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# アフィリエイト設定
AMAZON_TAG = "beautyhack-22"
RAKUTEN_ID = "51ff718c.e0bde7a9.51ff718d.6408b951"

JST = timezone(timedelta(hours=9))


def clean_url(url):
    """URLの末尾のゴミ文字（絵文字、ゼロ幅文字、ORC U+FFFC等）を除去"""
    url = url.strip()
    while url and (ord(url[-1]) > 127 or ord(url[-1]) < 33):
        url = url[:-1]
    url = url.rstrip('.,;:!?）)」』】>》')
    return url


def resolve_short_url(short_url):
    """短縮URL（amzn.to, a.r10.to等）を展開してリダイレクト先を取得"""
    short_url = clean_url(short_url)
    try:
        with httpx.Client(follow_redirects=False, timeout=10) as client:
            resp = client.head(short_url)
            loc = resp.headers.get("location", "")
            if loc and loc.startswith("http"):
                return loc
            return short_url
    except Exception:
        return short_url


def convert_amazon_url(url):
    """AmazonURLをジーマのアフィコード付きに変換"""
    url = clean_url(url)
    # 短縮URL展開
    if "amzn.to" in url or "amzn.asia" in url:
        url = resolve_short_url(url)

    # ASINを抽出
    asin_match = re.search(r'/(?:dp|gp/product)/([A-Z0-9]{10})', url)
    if asin_match:
        asin = asin_match.group(1)
        return f"https://www.amazon.co.jp/dp/{asin}?tag={AMAZON_TAG}"

    # ASIN取れない場合はtagだけ付け替え
    if "amazon.co.jp" in url or "amazon.com" in url:
        # 既存のtagを除去
        url = re.sub(r'[?&]tag=[^&]*', '', url)
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}tag={AMAZON_TAG}"

    return url


def convert_rakuten_url(url):
    """楽天URLをジーマのアフィコード付きに変換"""
    url = clean_url(url)
    # 短縮URL展開
    if "a.r10.to" in url:
        url = resolve_short_url(url)

    # 既にアフィリンクの場合はIDを差し替え
    if "hb.afl.rakuten.co.jp" in url:
        return re.sub(r'/hgc/[^/]+/', f'/hgc/{RAKUTEN_ID}/', url)

    # 楽天ROOMは除外
    if "room.rakuten.co.jp" in url:
        return ""

    # 通常の楽天URLをアフィリンクでラップ
    return f"https://hb.afl.rakuten.co.jp/hgc/{RAKUTEN_ID}/?pc={quote(url)}"


def rewrite_text(client, original):
    """投稿文をリライト"""
    resp = client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=1024,
        messages=[{"role": "user", "content": f"""以下の投稿文をリライトしてください。

【元の投稿文】
{original}

【ルール】
- 言い回しだけ軽く変える。構成・改行はほぼそのまま
- ハッシュタグは使わない
- 外国語の場合は自然な日本語に翻訳
- リライト結果だけを返して"""}],
    )
    return resp.content[0].text.strip()


def rewrite_reply(client, original_reply, post_text, amazon_urls, rakuten_urls):
    """リプライをリライト+アフィURL付与"""
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

    # 楽天URLをジーマのアフィコードに変換
    converted_rakuten = [convert_rakuten_url(u) for u in rakuten_urls if u]
    converted_rakuten = [u for u in converted_rakuten if u]  # 空文字除去（楽天ROOM等）
    if converted_rakuten:
        parts.append("楽天PR")
        parts.append(converted_rakuten[0])
        parts.append("")

    # AmazonURLをジーマのアフィコードに変換
    converted_amazon = [convert_amazon_url(u) for u in amazon_urls if u]
    if converted_amazon:
        parts.append("amazonPR")
        parts.append(converted_amazon[0])

    return "\n".join(parts).strip()


def main():
    if not ACCOUNTS:
        print("ACCOUNTS_JSON が設定されていません")
        return

    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)  # 元データタブ

    all_rows = ws.get_all_records()
    headers = ws.row_values(1)

    # 必要な列を確認/追加
    needed_cols = {
        "リライト結果": None,
        "リプライ結果": None,
        "担当垢": None,
        "投稿予定時刻": None,
        "投稿済み": None,
    }
    for i, h in enumerate(headers):
        if h in needed_cols:
            needed_cols[h] = i + 1

    # 足りない列を追加
    next_col = len(headers) + 1
    for col_name, col_idx in needed_cols.items():
        if col_idx is None:
            needed_cols[col_name] = next_col
            ws.update_cell(1, next_col, col_name)
            next_col += 1

    # 投稿テキストがある行を収集
    post_rows = []
    for i, row in enumerate(all_rows):
        if row.get("投稿文", ""):
            post_rows.append((i + 2, row))  # (row_number, data)

    total = len(post_rows)
    print(f"総ストック: {total}件")
    print(f"稼働垢: {len(ACCOUNTS)}垢")

    if total == 0:
        print("ストックなし")
        return

    # 投稿予定時刻を計算（10分間隔で143件）
    now = datetime.now(JST)
    today_start = now.replace(hour=0, minute=30, second=0, microsecond=0)
    interval_minutes = (23 * 60) / total  # 23時間で全件回す（0:30〜23:30）

    # 担当垢を割り当て（ラウンドロビン）
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # バッチ更新用データ
    updates = []

    for idx, (row_num, row) in enumerate(post_rows):
        account = ACCOUNTS[idx % len(ACCOUNTS)]
        scheduled_time = today_start + timedelta(minutes=interval_minutes * idx)
        time_str = scheduled_time.strftime("%H:%M")

        original_text = row.get("投稿文", "")
        original_reply = row.get("リプライ文言", "")
        amazon_url = row.get("amazonURL", "")
        rakuten_url = row.get("楽天URL", "")
        amazon_list = amazon_url.split() if amazon_url else []
        rakuten_list = rakuten_url.split() if rakuten_url else []

        # リライト
        try:
            rewritten_text = rewrite_text(client, original_text)
        except Exception as e:
            print(f"  [{idx+1}/{total}] リライト失敗: {e}")
            rewritten_text = original_text

        # リプライ（URLがある場合のみ）
        rewritten_reply = ""
        if (amazon_list or rakuten_list) and original_reply:
            try:
                rewritten_reply = rewrite_reply(client, original_reply, original_text, amazon_list, rakuten_list)
            except Exception as e:
                print(f"  [{idx+1}/{total}] リプライリライト失敗: {e}")
                rewritten_reply = original_reply

        # 更新データ
        updates.append({
            "range": f"{_col_letter(needed_cols['リライト結果'])}{row_num}",
            "values": [[rewritten_text]],
        })
        updates.append({
            "range": f"{_col_letter(needed_cols['リプライ結果'])}{row_num}",
            "values": [[rewritten_reply]],
        })
        updates.append({
            "range": f"{_col_letter(needed_cols['担当垢'])}{row_num}",
            "values": [[f"@{account['name']}"]],
        })
        updates.append({
            "range": f"{_col_letter(needed_cols['投稿予定時刻'])}{row_num}",
            "values": [[time_str]],
        })
        # 投稿済みをリセット
        updates.append({
            "range": f"{_col_letter(needed_cols['投稿済み'])}{row_num}",
            "values": [[""]],
        })

        print(f"  [{idx+1}/{total}] @{account['name']} {time_str} | {rewritten_text[:40]}...")

        # 50件ごとにバッチ書き込み（API制限対策）
        if len(updates) >= 250:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            updates = []

    # 残りを書き込み
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    print(f"\n完了! {total}件リライト+スケジュール設定")


def _col_letter(col_num):
    """列番号をアルファベットに変換（1=A, 27=AA）"""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


if __name__ == "__main__":
    main()
