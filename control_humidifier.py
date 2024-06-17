# Databricks notebook source
# MAGIC %md
# MAGIC # Switchbot Humilizerの操作

# COMMAND ----------

catalog = 'skato'
schema = 'prod_co2'
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 加湿器操作ログテーブル作成

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS logs_control_humidifier (
  event_timestamp TIMESTAMP,
  command STRING,
  res STRING
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Switch Bot API実行

# COMMAND ----------

# DBTITLE 1,API Config
import time
import hashlib
import hmac
import base64
import requests
import uuid

def generate_headers(secret_scope = 'skato-scope'):
  """
  SwitchBotアプリから取得したトークンとシークレットキーを使用して、APIリクエストのヘッダーを生成します。

  Returns:
    headers (dict): APIリクエストのヘッダー情報
  """
  # SwitchBotアプリから取得したトークンとシークレットキーを設定
  token = dbutils.secrets.get(scope = secret_scope, key = 'switchbot_token')
  secret = dbutils.secrets.get(scope = secret_scope, key = 'switchbot_secret')

  # 現在のタイムスタンプをミリ秒単位で取得
  t = int(round(time.time() * 1000))

  # ランダムなUUIDを生成
  nonce = str(uuid.uuid4())

  # 署名を生成
  string_to_sign = bytes(f'{token}{t}{nonce}', 'utf-8')
  secret_bytes = bytes(secret, 'utf-8')
  sign = base64.b64encode(hmac.new(secret_bytes, msg=string_to_sign, digestmod=hashlib.sha256).digest()).decode('utf-8')

  # APIリクエストのヘッダーを設定
  headers = {
      'Authorization': token,
      'sign': sign,
      'nonce': nonce,
      't': str(t),
      'Content-Type': 'application/json; charset=utf8'
  }

  return headers

# COMMAND ----------

# DBTITLE 1,Get Device ID
# GETリクエストを送信しデバイス一覧を取得
response = requests.get('https://api.switch-bot.com/v1.1/devices', headers=generate_headers())

# 加湿器のデバイスIDを取得
data = response.json()
device_dict = data.get('body', {}).get('deviceList', [])
humidifiers = list(filter(lambda device: device['deviceType'] == 'Humidifier', device_dict))
humidifier_id = humidifiers[0]['deviceId']

# COMMAND ----------

# DBTITLE 1,Get present value of humidity
humidity_value = spark.sql("""
  SELECT humidity
  FROM skato.prod_co2.iot_silver
  ORDER BY event_timestamp_jst DESC
  LIMIT 1
""").collect()[0][0]
humidity_value = float(humidity_value)

# COMMAND ----------

# DBTITLE 1,Define function 'control_humidifier'
def control_humidifier(humidifier_status, humidity_value, thres_low=40, thres_high=60):
  """
    加湿器の状態と現在の湿度値を基に、加湿器を制御する関数です。
    加湿器がオフの状態で湿度が低い場合、または加湿器がオンの状態で湿度が高い場合に、加湿器をオン/オフします。
    
    Parameters:
    - humidifier_status (dict): 加湿器の現在の状態を含む辞書。'power'キーで加湿器がオンかオフかを示します。
    - humidity_value (float): 現在の湿度値。
    - thres_low (int, optional): 加湿器をオンにする湿度の下限閾値。デフォルトは40。
    - thres_high (int, optional): 加湿器をオフにする湿度の上限閾値。デフォルトは60。
    
    Returns:
    - command (str): 加湿器に送るコマンド('turnOn'または'turnOff')。
    - res (dict): コマンド実行後の加湿器の状態。
    """
  
  condition = (
      (humidity_value < thres_low and humidifier_status['power'] == 'off') or 
      (humidity_value > thres_high and humidifier_status['power'] == 'on')
  )

  if condition:
      command = 'turnOff' if humidifier_status['power'] == 'on' else 'turnOn'
      body = {
      'command': command,
      'parameter': 'default',
      'commandType': 'command'
      }
      response = requests.post(f'https://api.switch-bot.com/v1.1/devices/{humidifier_id}/commands', headers=generate_headers(), json=body)
      res = response.json()
  else:
    command = 'none'
    res = {}

  return command, res

# COMMAND ----------

# DBTITLE 1,Smart Humidifier Control and Logging with Spark
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 加湿器のステータスを取得
current_time = datetime.now()
response = requests.get(f'https://api.switch-bot.com/v1.1/devices/{humidifier_id}/status', headers=generate_headers())
humidifier_status = response.json().get('body', {})

# 加湿器の操作コマンドをリクエストし、実行したコマンドとレスポンスを取得
command, res = control_humidifier(humidifier_status=humidifier_status, humidity_value=humidity_value)

# 操作結果をテーブルに記録
schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("command", StringType(), True),
    StructField("res", StringType(), True)
])
rows = [(current_time, command, str(res))]
df = spark.createDataFrame(rows, schema=schema)

# Insert the DataFrame into the Spark table
df.write.mode("append").saveAsTable("logs_control_humidifier")
