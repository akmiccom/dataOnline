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


GRAPE_CONSTANTS = {
    "マイジャグラーV": {
        "bb": 239.25,
        "rb": 95.25,
        "replay": 0.411,
        "cherryOff": 0.05847,
        "cherryOn": 0.04228,
    },
    "アイムジャグラーEX-TP": {
        "bb": 251.25,
        "rb": 95.25,
        "replay": 0.411,
        "cherryOff": 0.06068,
        "cherryOn": 0.040475,
    },
    "ゴーゴージャグラー3": {
        "bb": 239.00,
        "rb": 95.00,
        "replay": 0.411,
        "cherryOff": 0.0661,
        "cherryOn": 0.0372,
    },
    "ファンキージャグラー2": {
        "bb": 239.25,
        "rb": 95.25,
        "replay": 0.411,
        "cherryOff": 0.0603,
        "cherryOn": 0.04324,
    },
}


SPREADSHEET_IDS = {
    "EXA FIRST": "10-B_vV1pvUzXmvGAiHhODGJgCloOsAmqSO9HvXpk_T8",
    "コンサートホールエフ成増": "1EDY2RfjDQNsapVrl2X-UrqPKoXrkQmYJnk3uPqccBxY",
    "ニュークラウン川越2号店": "1jocaH94-5GKWUqEqcrybgFcpb1Tn0BhvLMqaADppNWg",
    "第一プラザ狭山店": "1IVb2Woq3n_PDZP87LdW9NpFP3Z6LeyQtONCkx_fWIq4",
    "第一プラザ坂戸1000": "170MVr-BB3LG-g5ItkDT-8TE6R68RW9zJhRfpvQiy-PE",
    "第一プラザみずほ台店": "1_1722pigi_Z1D6eH0tsPfMneGoS9O09fyqD6F-h1mQA",
    "みずほ台uno": "1PhH3DbHjUTmsE0yJ7U85KKeFwqtSD7x7VZvTD7ZQO74",
    "第一プラザ坂戸にっさい店": "1IZrivGLeE30LgT1VvHhqcgLkOMcmdy-4GIC-4KEchbE",
    "オータ志木駅前店": "1d5UhcseEl7jscVqEk-u3-DZgi5EmIFnMhd6jfrPbpzw",
    "グランドオータ新座駅前店": "1cPAH5IW_qlViRKCc0eR1m-NN8eBuJ_0sjrDDYT_DWas",
    "toho川越店": "1VOr7BNMB-xnHihZrk6MFmetlZ0nCFP86YcrJu9w0dac",
    "ニューダイエイiii": "1CBDcxHjGT11_ujn0v_m-xDIZKJ6Xw1RaFzGdOt3jFVo",
    "レイト平塚": "1WlvgMk-u_tUWizQVOYDQVg7bECyVzLm0-wlJDgJteAw",
    "パールショップともえ川越店": "1i70joJ27Hs7inS-D89z9YMSJO1aRvaBeeWn0n9xpktY",
    "楽園大山店": "1dewRwbCe783UAj7C_r5tn4CA7JyPfwxCahc8aOf9M2I",
    "パラッツォ川越店": "179nJF0NvLng7xPKsd_NX2pJBXsDNsO8SJhOvUAvFk2I",
    "スロットエランドール田無店": "1czMHMqEjt9__Ou6tyKnWxhFa6atfhjeSDaFbf1Ef-c8",
    "sapみずほ台": "1zTVmFbPS7HRmXe8VlJczPXl407BCp8fMZPZJdz1e23Y",
    "マルハン大山店": "14EO39e7xjHu-H7kBOTz3ZI3M0RN6hkbFIN8XD-ZKmPE",
    "ミリオン和光10号店": "1UnWFvjjPS3kcxzFU5Pz0DdqCGK0ACVd1UwUc8FYlK8A",
    "クラウンときわ台": "1ePS8wT2FaQwB9tpOm9Qgh1fY_fXM6_7usHFrMrm6XCQ",
    "大山オーシャン": "1sJMqNDHE2eGzrRnaCSGUM60ky-DqE-rGK7L_e3R-N0w",
}
