import sqlite3

# SQLiteファイル（データベース）の作成・接続
DB_NAME = "anaslo_02/anaslo_02.db"
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# --- テーブル作成 ---

# 都道府県テーブル
cursor.execute("""
CREATE TABLE IF NOT EXISTS prefectures (
    prefecture_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
""")

# ホールテーブル（prefecture_id に変更）
cursor.execute("""
CREATE TABLE IF NOT EXISTS halls (
    hall_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    prefecture_id INTEGER NOT NULL,
    FOREIGN KEY (prefecture_id) REFERENCES prefectures(prefecture_id),
    UNIQUE (name, prefecture_id)
);
""")

# 機種テーブル
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS models (
    model_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
"""
)

# 出玉データテーブル
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hall_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL,
    unit_no INTEGER NOT NULL,
    date TEXT NOT NULL,
    game INTEGER,
    BB INTEGER,
    RB INTEGER,
    medals INTEGER,
    FOREIGN KEY (hall_id) REFERENCES halls(hall_id),
    FOREIGN KEY (model_id) REFERENCES models(model_id)
);
"""
)

# 出玉データのユニーク制約（ホール・台番号・日付の重複を防ぐ）
cursor.execute(
    """
CREATE UNIQUE INDEX IF NOT EXISTS idx_results_unique
ON results (hall_id, unit_no, date);
"""
)

# コミットして終了
conn.commit()
conn.close()

print(f"✅ データベース {DB_NAME} を作成しました。")
