import sqlite3

# データベースファイル名
DB_NAME = "anaslo_02/anaslo_02.db"

# 47都道府県リスト
prefectures = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県"
]

# データベース接続・カーソル作成
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# 都道府県を一括で追加（重複はスキップ）
for name in prefectures:
    cursor.execute("INSERT OR IGNORE INTO prefectures (name) VALUES (?)", (name,))

# コミットと終了処理
conn.commit()
conn.close()

print("✅ 都道府県47件を prefecutres テーブルに登録しました。")
