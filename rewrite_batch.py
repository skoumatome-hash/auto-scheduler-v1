"""毎日0時に実行: 143投稿全部をリライト+担当垢+投稿予定時刻を元データタブに書く"""
import json
import os
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, unquote

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
    """URLの末尾のゴミ文字（絵文字、ゼロ幅文字、ORC U+FFFC等）を除去 + l.threads.comデコード"""
    url = url.strip()
    while url and (ord(url[-1]) > 127 or ord(url[-1]) < 33):
        url = url[:-1]
    url = url.rstrip('.,;:!?）)」』】>》')
    # l.threads.comのリダイレクトURLから実URLを抽出
    if "l.threads.com" in url or "l.threads.net" in url:
        m = re.search(r'[?&]u=([^&]+)', url)
        if m:
            url = unquote(m.group(1))
    return url


def resolve_short_url(short_url):
    """短縮URL（amzn.to, a.r10.to等）を全段展開してリダイレクト先を取得"""
    short_url = clean_url(short_url)
    try:
        with httpx.Client(follow_redirects=True, timeout=10) as client:
            resp = client.head(short_url)
            final = str(resp.url)
            if final and final.startswith("http"):
                return final
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
    if not url:
        return ""

    # 楽天以外のURL（Amazon等）が混入してたら無視
    if "amazon" in url or "amzn" in url:
        return ""

    # 短縮URL展開
    if "a.r10.to" in url:
        url = resolve_short_url(url)

    # 楽天ドメインじゃなければ無視
    if not any(d in url for d in ["rakuten.co.jp", "rakuten.ne.jp", "r10.to", "afl.rakuten"]):
        return ""

    # 楽天アフィリンクの中身（リダイレクト先）にAmazonが含まれてたら無視
    from urllib.parse import unquote
    decoded = unquote(unquote(url))
    if "amazon" in decoded.lower():
        return ""

    # 既にアフィリンクの場合はIDを差し替え
    if "hb.afl.rakuten.co.jp" in url:
        return re.sub(r'/hgc/[^/]+/', f'/hgc/{RAKUTEN_ID}/', url)

    # 楽天ROOMは除外
    if "room.rakuten.co.jp" in url:
        return ""

    # 通常の楽天URLをアフィリンクでラップ
    return f"https://hb.afl.rakuten.co.jp/hgc/{RAKUTEN_ID}/?pc={quote(url)}"


def _api_call_with_retry(fn, max_retries=3):
    """Claude API呼び出し（500エラー時にリトライ）"""
    import time as _t
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if '500' in str(e) or '529' in str(e) or 'overloaded' in str(e).lower():
                wait = 10 * (attempt + 1)
                print(f"  API Error (attempt {attempt+1}/{max_retries}): {str(e)[:60]}... {wait}秒待機")
                _t.sleep(wait)
            else:
                raise
    return fn()


def rewrite_text(client, original):
    """投稿文をリライト"""
    resp = _api_call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=1024,
        messages=[{"role": "user", "content": f"""以下の投稿文をリライトしてください。

【元の投稿文】
{original}

【ルール】
- 言い回しだけ軽く変える。構成・改行はほぼそのまま
- ハッシュタグは使わない
- 外国語の場合は自然な日本語に翻訳
- **必ず500文字以内に収める**（Threads APIの制限）
- リライト結果だけを返して"""}],
    ))
    return resp.content[0].text.strip()


def rewrite_reply(client, original_reply, post_text, amazon_urls, rakuten_urls):
    """リプライをリライト+アフィURL付与（強垢風シンプルフォーマット）"""
    resp = _api_call_with_retry(lambda: client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=512,
        messages=[{"role": "user", "content": f"""以下の投稿に合う商品紹介の一言を書いてください。

【投稿内容】
{post_text[:200]}

【参考（元のリプライ）】
{original_reply}

【ルール】
- 1〜2行で超短く（「これマジで良かった」「気になる人はこちら」レベル）
- 自然な口語体。押し売り感なし
- 「楽天」「amazon」「PR」「ad」等のラベルは絶対書くな
- URLは絶対書くな（こちらで付ける）
- 紹介の一言だけ返して"""}],
    ))
    intro = resp.content[0].text.strip()
    # 紹介文からURL・ラベル残りを除去
    intro = re.sub(r'https?://[^\s\u3000]+', '', intro)
    intro = re.sub(r'(?i)(楽天|amazon|amzn|rakuten|PR|ad)\s*(pr|PR)?', '', intro)
    intro = re.sub(r'\n{2,}', '\n', intro).strip()

    # URL付与（強垢風: 紹介文 + URL pr）
    parts = [intro]
    # 楽天とAmazonの重複排除
    rak_unique = list(dict.fromkeys(u.strip() for u in rakuten_urls if u.strip()))[:1]
    amz_unique = list(dict.fromkeys(u.strip() for u in amazon_urls if u.strip()))[:1]

    if rak_unique:
        parts.append(f"{rak_unique[0]} pr")
    if amz_unique:
        parts.append(f"{amz_unique[0]} pr")

    result = "\n".join(parts).strip()

    # URLが1つも入ってなければ空返し（リプライ投稿しない）
    if not rak_unique and not amz_unique:
        return ""

    # 安全チェック: W列のURLがI列/J列のものだけか確認
    all_urls_in_result = re.findall(r'https?://[^\s\u3000]+', result)
    allowed_urls = set(rak_unique + amz_unique)
    for url in all_urls_in_result:
        clean_url = url.rstrip('.,;:')
        if clean_url not in allowed_urls:
            print(f"  CRITICAL: 許可外URL検出 {clean_url} -> 除去")
            result = result.replace(url, '')

    # 楽天IDチェック
    if RAKUTEN_ID.split('.')[0] not in result and "hb.afl.rakuten" in result:
        print(f"  WARNING: 楽天IDが不正")

    # 500文字制限: URLは削れないから紹介文を短縮
    if len(result) > 500:
        url_part = result[len(intro):]
        max_intro = 500 - len(url_part) - 10
        if max_intro > 20:
            intro = intro[:max_intro].rsplit("。", 1)[0] + "。"
        else:
            intro = intro[:50]
        result = (intro + url_part).strip()
    return result


def _write_summary(sh, total_posts, total_accounts, interval_min):
    """投稿設定タブに今日のサマリーを書き出し"""
    try:
        try:
            ws = sh.worksheet("投稿設定")
        except Exception:
            ws = sh.add_worksheet(title="投稿設定", rows=20, cols=4)

        now = datetime.now(JST)
        posts_per_hour = max(1, 60 // interval_min)
        hours_needed = total_posts / posts_per_hour if posts_per_hour > 0 else 24
        posts_per_account = total_posts // total_accounts if total_accounts > 0 else 0

        data = [
            ["項目", "値", "", ""],
            ["更新日時", now.strftime("%Y-%m-%d %H:%M"), "", ""],
            ["", "", "", ""],
            ["総ストック数", f"{total_posts}件", "", ""],
            ["稼働アカウント数", f"{total_accounts}垢", "", ""],
            ["1垢あたり投稿数", f"{posts_per_account}件/日", "", ""],
            ["投稿間隔", f"{interval_min}分", "", ""],
            ["1時間あたり投稿数", f"約{posts_per_hour}件", "", ""],
            ["全投稿完了予想", f"約{hours_needed:.1f}時間", "", ""],
            ["投稿時間帯", "0:30 〜 23:50", "", ""],
            ["", "", "", ""],
            ["1回の実行上限", "10件", "", ""],
            ["トリガー", "GAS 毎時（:44分頃）", "", ""],
        ]

        ws.update(values=data, range_name="A1:D13", value_input_option="USER_ENTERED")
        print(f"投稿設定タブ更新: {total_posts}件/{total_accounts}垢/{interval_min}分間隔")

    except Exception as e:
        print(f"投稿設定タブ更新エラー: {e}")


def _save_post_log(sh, all_rows):
    """前日の投稿結果を「投稿ログ」タブに保存（1週間分保持）"""
    try:
        try:
            log_ws = sh.worksheet("投稿ログ")
        except Exception:
            log_ws = sh.add_worksheet(title="投稿ログ", rows=2000, cols=8)
            log_ws.update(
                values=[["日付", "No", "アカウント", "投稿日時", "投稿ID", "投稿URL", "ステータス", "投稿文（先頭50字）"]],
                range_name="A1:H1",
                value_input_option="USER_ENTERED",
            )

        # 投稿済みの行を収集
        new_logs = []
        for row in all_rows:
            status = str(row.get("ステータス", ""))
            if not status:
                continue
            new_logs.append([
                str(row.get("投稿日時", "")),
                str(row.get("No", "")),
                str(row.get("投稿アカウント", "")),
                str(row.get("投稿日時", "")),
                str(row.get("投稿ID", "")),
                str(row.get("投稿URL", "")),
                status,
                str(row.get("リライト結果", ""))[:50],
            ])

        if new_logs:
            # 末尾に追記
            existing = log_ws.get_all_values()
            next_row = len(existing) + 1
            log_ws.update(
                values=new_logs,
                range_name=f"A{next_row}:H{next_row + len(new_logs) - 1}",
                value_input_option="USER_ENTERED",
            )
            print(f"投稿ログ保存: {len(new_logs)}件")

            # 1週間超え（7日前より古い）を削除
            cutoff = (datetime.now(JST) - timedelta(days=7)).strftime("%Y-%m-%d")
            all_log = log_ws.get_all_values()
            # ヘッダー除外、日付が古い行を特定
            rows_to_delete = []
            for i, log_row in enumerate(all_log[1:], start=2):
                if log_row[0] and log_row[0] < cutoff:
                    rows_to_delete.append(i)
            # 下から削除（行番号ずれ防止）
            for row_idx in reversed(rows_to_delete):
                log_ws.delete_rows(row_idx)
            if rows_to_delete:
                print(f"古いログ削除: {len(rows_to_delete)}件（{cutoff}以前）")
        else:
            print("投稿ログ: 保存対象なし")

    except Exception as e:
        print(f"投稿ログ保存エラー: {e}")


def main():
    if not ACCOUNTS:
        print("ACCOUNTS_JSON が設定されていません")
        return

    gc = gspread.service_account_from_dict(GCP_CREDS)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)  # 元データタブ

    all_rows = ws.get_all_records()
    headers = ws.row_values(1)

    # 前日の投稿結果を「投稿ログ」タブに保存
    _save_post_log(sh, all_rows)

    # 必要な列を確認/追加
    needed_cols = {
        "リライト結果": None,
        "リプライ結果": None,
        "担当垢": None,
        "投稿予定時刻": None,
        "投稿アカウント": None,
        "投稿日時": None,
        "投稿ID": None,
        "投稿URL": None,
        "ステータス": None,
        "amazonURL": None,
        "楽天URL": None,
    }
    for i, h in enumerate(headers):
        if h in needed_cols:
            needed_cols[h] = i + 1

    # 足りない列を追加（amazonURL/楽天URLは元データの既存列なので追加しない）
    existing_only_cols = {"amazonURL", "楽天URL"}
    next_col = len(headers) + 1
    for col_name, col_idx in needed_cols.items():
        if col_idx is None and col_name not in existing_only_cols:
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

    # 投稿予定時刻を計算（日付付きでセット）
    now = datetime.now(JST)
    today_start = now.replace(hour=0, minute=30, second=0, microsecond=0)
    interval_minutes = max(10, (23 * 60) // total)  # 143件なら約10分間隔

    # 毎日シャッフル（同じNoが毎日同じ時間帯にならないように）
    import random
    random.shuffle(post_rows)

    # 担当垢を割り当て（ラウンドロビン）
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # バッチ更新用データ
    updates = []

    for idx, (row_num, row) in enumerate(post_rows):
        account = ACCOUNTS[idx % len(ACCOUNTS)]
        scheduled_time = today_start + timedelta(minutes=interval_minutes * idx)
        time_str = scheduled_time.strftime("%Y-%m-%d %H:%M")

        original_text = row.get("投稿文", "")
        original_reply = row.get("リプライ文言", "")
        amazon_url = row.get("amazonURL", "")
        rakuten_url = row.get("楽天URL", "")
        # 改行・スペース・カンマ区切りで複数URL対応
        amazon_list = [u.strip() for u in re.split(r'[\n\s,]+', amazon_url) if u.strip() and ('amazon' in u or 'amzn' in u)]
        rakuten_list = [u.strip() for u in re.split(r'[\n\s,]+', rakuten_url) if u.strip() and ('rakuten' in u or 'r10.to' in u)]
        # 3つ以上は先頭2つだけ使う
        if len(amazon_list) > 2:
            print(f"  [{idx+1}] Amazon URL {len(amazon_list)}個 -> 2個に制限")
            amazon_list = amazon_list[:2]
        if len(rakuten_list) > 2:
            print(f"  [{idx+1}] 楽天 URL {len(rakuten_list)}個 -> 2個に制限")
            rakuten_list = rakuten_list[:2]

        # リライト
        try:
            rewritten_text = rewrite_text(client, original_text)
        except Exception as e:
            print(f"  [{idx+1}/{total}] リライト失敗: {e}")
            rewritten_text = original_text

        # リプライ（URLがある場合のみ）
        rewritten_reply = ""
        if amazon_list or rakuten_list:
            # H列が空でもI列/J列にURLがあればリプライを自動生成
            if not original_reply:
                parts = []
                if rakuten_list:
                    parts.append("楽天PR\n" + rakuten_list[0])
                if amazon_list:
                    parts.append("amazonPR\n" + amazon_list[0])
                original_reply = "\n\n".join(parts)
            try:
                rewritten_reply = rewrite_reply(client, original_reply, original_text, amazon_list, rakuten_list)
            except Exception as e:
                print(f"  [{idx+1}/{total}] リプライリライト失敗: {e}")
                rewritten_reply = original_reply

        # 最終URLチェック（W列に書く前にタグ確認）
        if rewritten_reply:
            final_bad = re.findall(r'tag=([a-z0-9_-]+)', rewritten_reply)
            final_bad = [t for t in final_bad if t != AMAZON_TAG]
            if final_bad:
                print(f"  [{idx+1}] CRITICAL: W列に他人タグ残存 {final_bad} -> 強制差替え")
                rewritten_reply = re.sub(r'tag=[a-z0-9_-]+', f'tag={AMAZON_TAG}', rewritten_reply)

        # I列/J列は触らない（事前にジーマのIDで短縮URL生成済み）

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
        # 投稿結果列をリセット（前日分をクリア → 今日分で上書きされる）
        for col_name in ["投稿アカウント", "投稿日時", "投稿ID", "投稿URL", "ステータス"]:
            if needed_cols.get(col_name):
                updates.append({
                    "range": f"{_col_letter(needed_cols[col_name])}{row_num}",
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

    # 投稿設定タブにサマリーを書き出し
    _write_summary(sh, total, len(ACCOUNTS), interval_minutes)

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
