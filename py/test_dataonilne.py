#!/usr/bin/env python
# coding: utf-8

# In[23]:


from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from subprocess import Popen
from time import sleep
import pandas as pd
import sqlite3
import os


# In[24]:


def start_google_chrome(url, port=9222, user_data_dir="C:/temp"):
    
    chrome_path = f'"C:\Program Files\Google\Chrome\Application\chrome.exe" -remote-debugging-port={port} --user-data-dir="{user_data_dir}"'
    Popen(chrome_path)
    sleep(1)

    options = ChromeOptions()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")

    driver = Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    driver.implicitly_wait(10)
    
    return driver


# In[25]:


def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS slot_data (
            date TEXT,
            hall_id INTEGER,
            model TEXT,
            unit_no INTEGER,
            start INTEGER,
            bb INTEGER,
            rb INTEGER,
            art INTEGER,
            max_medals INTEGER,
            bb_rate TEXT,
            rb_rate TEXT,
            art_rate TEXT,
            total_rate TEXT,
            last_start INTEGER,
            UNIQUE(date, unit_no)
        )
    """)
    conn.commit()


# In[26]:


URL = f"https://daidata.goraggio.com/"
STORES = {"EXA FIRST" : 101262}
hall_id = STORES["EXA FIRST"]

url = URL + str(hall_id)
print(url)


# In[27]:


driver = start_google_chrome(url)


# In[28]:


store_name = driver.title.replace(" - 台データオンライン", "")
store_name = store_name.replace(" ", "_")
print(store_name)


# In[29]:


# 機種名入力
search_word = "ジャグラー"
input_box = driver.find_element(By.NAME, "machine_name")
if input_box:
    input_box.clear()
    sleep(0.5)
    input_box.send_keys(search_word, Keys.ENTER)


# In[30]:


# 広告対策
try:
    ele = driver.find_element(By.XPATH, '//button[text()="close"]').click()
except NoSuchElementException:
    pass


# In[31]:


# 機種名取得して n 番目をクリック
n = 0
models = driver.find_elements(By.CLASS_NAME, "model_name")
model_names = [model.text for model in models]
model_name = model_names[n].replace(" ", "_")
models[n].click()

WebDriverWait(driver, 10).until(
    EC.invisibility_of_element_located((By.CLASS_NAME, "table.sorter"))
)


# In[32]:


# 日付変更
DAYS_AGO = 1
select_elem = driver.find_element(By.NAME, "hist_num")
select = Select(select_elem)
dates = [option.text for option in select.options]

select.select_by_visible_text(dates[DAYS_AGO])
print(dates[DAYS_AGO])

WebDriverWait(driver, 10).until(
    EC.invisibility_of_element_located((By.CLASS_NAME, "table.sorter"))
)


# In[33]:


# ヘッダー（固定）

# データフレーム化・保存
file_name = f"../csv/{store_name}_{model_name}_{dates[DAYS_AGO]}.csv"
 
if os.path.exists(file_name):
    print(f"データは既に保存されています： {file_name}")
    df = pd.read_csv(file_name)
else:
    # テーブル取得
    rows = driver.find_elements(By.CSS_SELECTOR, 'table tr')
    
    columns = rows[0].text.split()
    data = []
    for row in rows[1:]:
        cols = row.find_elements(By.TAG_NAME, 'td')
        
        if cols:
            data.append([col.text.strip() for col in cols[1:]])
    
    print(f"{len(data)} 件のデータ取得完了")
    
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"データ保存完了： {file_name}")


# In[34]:


driver.quit()


# In[35]:


# データベースに接続（ファイルがなければ作成される）
DB_PATH = "../db/pachislo.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
conn.commit()

# for row in conn.execute("PRAGMA table_info(slot_data)"):
#     print(row)


# In[36]:


df["date"] = dates[DAYS_AGO]
df["hall_id"] = hall_id
df["model"] = model_name


# In[37]:


df = df.rename(columns={
    "台番号": "unit_no", "累計スタート": "start", "BB回数": "bb", "RB回数": "rb",
    "ART回数": "art", "最大持玉": "max_medals", "BB確率": "bb_rate", "RB確率": "rb_rate",
    "ART確率": "art_rate", "合成確率": "total_rate", "前日最終スタート": "last_start"
})

expected_columns = [
    "date",
    "hall_id",
    "model",
    "unit_no",
    "start",
    "bb",
    "rb",
    "art",
    "max_medals",
    "bb_rate",
    "rb_rate",
    "art_rate",
    "total_rate",
    "last_start",
]

df = df[[col for col in expected_columns if col in df.columns]]

df.head()


# In[38]:


# SQLiteに書き込む
conn = sqlite3.connect(DB_PATH)
create_table(conn)
cursor = conn.cursor()
for _, row in df.iterrows():
    cursor.execute(
        """
        INSERT OR IGNORE INTO slot_data (
            date, hall_id, model, unit_no, start, bb, rb, art,
            max_medals, bb_rate, rb_rate, art_rate, total_rate, last_start
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(row[col] for col in expected_columns),
    )
conn.close()


# In[ ]:




