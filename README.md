# パチスロデータ取得

## 定数データ（config.py などで一元化）

- 初期URL
- 対象となる都道府県
- ホール名
- 取得期間
- データベースのパス
- csvのパス
- マジックナンバーなし

## スクレイピング＋CSV保存（scrape_and_save.py 前処理なし）

- CSVの整形はせず抽出データをそのまま保存
- ChromeDriver設定
    - Options()（ヘッドレス、User-Agentなど）
    - ウインドウサイズ
- スクレイピング防止対策
    - Waitの設定
    - スクロール設定
- 広告対策
    - 例：try: driver.find_element(...).click()
- logger設定
    - logging によるログ出力（時間・処理結果を記録）

## csv の前処理及び、データベースに保存するファイル

### 不要情報の削除

- 不要情報の削除
    - 不要な列の削除（例：出力日、空白列など）
    - カンマ・空白・+・円などの記号削除

### DB テーブル構成

- prefectures
    - prefecture_id (PK)
    - name

- halls
    - hall_id (PK)
    - name

- models
    - model_id (PK)
    - name

- results
    - hall_id (PK)
    - model_id (FK)
    - date
    - game
    - BB
    - RB
    - medals

### ユニーク制約

- UNIQUE (hall_id, unit_no, date)：1台あたり1日1件のみ

## データベース分析用（analyze.py など）

### 分析用項目の算出

- 確率カラム
    - total_rate = (BB + RB) / games
    - bb_rate = BB / games
    - rb_rate = RB / games
- 日付分解カラム
    - month（YYYY-MM）
    - day（DD）
    - weekday（日本語で月曜〜日曜 or 数値0〜6）

### 統計データの抽出

## ディレクトリ構成

```arduino
pachislo_project/
│
├─ config/                      ← 初期設定（定数）
│   └─ config.py
│
├─ scraper/
│   └─ scrape_and_save.py       ← スクレイピング処理
│
├─ preprocess/
│   └─ insert_with_pandas.py    ← CSV整形＆DB登録
│
├─ analysis/
│   └─ analyze.py               ← 分析・可視化
│
├─ data/
│   └─ 東京都_EXA_FIRST_2025-04-03.csv
│
├─ anaslo_02/
│   └─ anaslo_02.db             ← SQLiteデータベース

```

## 将来的な拡張も意識

- 日付の自動取得＆スケジューリング（例：cron + headless Chrome）
- Next.jsでWeb可視化
- XGBoostやランダムフォレストによる設定推定モデル
