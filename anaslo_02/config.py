from datetime import date

today = date.today()

MAX_RETRIES = 5
SLEEP_TIME = 5

DB_PATH = r"anaslo_02/db/anaslo_02.db"
CSV_PATH = r"anaslo_02/csv/"
ARCHIVE_PATH = r"anaslo_02/archive/"
LOG_PATH = r"anaslo_02/log/anaslo.log"
JSONF = r"anaslo_02/json/spreeadsheet-347321-ff675ab5ccbd.json"
# AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/{SEARCH_WORD}_area_map.json"
# AREA_MAP_PATH = r"anaslo_02/json/exa_area_map.json"

SPREADSHEET_IDS = {
    "EXA FIRST": "10-B_vV1pvUzXmvGAiHhODGJgCloOsAmqSO9HvXpk_T8",
    "コンサートホールエフ成増": "1EDY2RfjDQNsapVrl2X-UrqPKoXrkQmYJnk3uPqccBxY",
    "パラッツォ川越店": "179nJF0NvLng7xPKsd_NX2pJBXsDNsO8SJhOvUAvFk2I",
    "第一プラザ狭山店": "1IVb2Woq3n_PDZP87LdW9NpFP3Z6LeyQtONCkx_fWIq4",
    "第一プラザ坂戸1000": "170MVr-BB3LG-g5ItkDT-8TE6R68RW9zJhRfpvQiy-PE",
    "第一プラザみずほ台店": "1_1722pigi_Z1D6eH0tsPfMneGoS9O09fyqD6F-h1mQA",
    "パールショップともえ川越店": "1i70joJ27Hs7inS-D89z9YMSJO1aRvaBeeWn0n9xpktY",
    "みずほ台uno": "1PhH3DbHjUTmsE0yJ7U85KKeFwqtSD7x7VZvTD7ZQO74",
    }

MODEL_LIST = [
        "マイジャグラーV",
        "ゴーゴージャグラー3",
        "アイムジャグラーEX-TP",
        "ファンキージャグラー2",
        "ミスタージャグラー",
        "ウルトラミラクルジャグラー",
        "ジャグラーガールズ",
        "ハッピージャグラーVIII",
    ]


QUERY = """
-- 出玉データにホール名と機種名を結合して取得
SELECT
    r.*, 
    h.name AS hall_name,     -- ホール名を追加
    m.name AS model_name     -- 機種名を追加
FROM results r
JOIN halls h ON r.hall_id = h.hall_id  -- ホールと結合
JOIN models m ON r.model_id = m.model_id  -- 機種と結合
WHERE h.name = ?  -- 指定ホールのみ
AND m.name LIKE '%ジャグラー%'  -- ジャグラー系機種に限定
ORDER BY r.date DESC, r.unit_no ASC;
"""
