MAX_RETRIES = 5
SLEEP_TIME = 5

DB_PATH = "anaslo_02/db/anaslo_02.db"
CSV_PATH = "anaslo_02/csv/"
ARCHIVE_PATH = "anaslo_02/archive/"
LOG_PATH = "anaslo_02/log/anaslo.log"
JSONF = "anaslo_02/json/spreeadsheet-347321-ff675ab5ccbd.json"

spreadSheet_ids = {
    "EXA FIRST": "10-B_vV1pvUzXmvGAiHhODGJgCloOsAmqSO9HvXpk_T8",
    "アスカ狭山店": "179nJF0NvLng7xPKsd_NX2pJBXsDNsO8SJhOvUAvFk2I",
    "パールショップともえ川越店": "1i70joJ27Hs7inS-D89z9YMSJO1aRvaBeeWn0n9xpktY",
    }


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