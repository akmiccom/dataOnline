import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# スプレッドシート認証設定
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
jsonf = r"C:\python\dataOnline\anaslo_02\spreeadsheet-347321-ff675ab5ccbd.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scope)
client = gspread.authorize(creds)
