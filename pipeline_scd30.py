# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline for IoT Sensor Data (RaspberryPi & SCD30)

# COMMAND ----------

# MAGIC %md
# MAGIC Azure IoT Hub経由でADLSに格納されたセンサーデータの取り込み・加工を行う

# COMMAND ----------

# DBTITLE 1,Config
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

ADLS_STORAGEACCOUNT_NAME                    = spark.conf.get("azure.adls.storageaccount.name")
ADLS_CONTAINER_NAME                         = spark.conf.get("azure.adls.container.name")
ADLS_ACCESSKEY_NAME                         = spark.conf.get("azure.adls.accesskey.name")
SECRET_SCOPE                                = spark.conf.get("dbx.secret.scope.name")

ADLS_ACCESSKEY = dbutils.secrets.get(scope = SECRET_SCOPE, key = ADLS_ACCESSKEY_NAME)

input_path = f"abfss://{ADLS_CONTAINER_NAME}@{ADLS_STORAGEACCOUNT_NAME}.dfs.core.windows.net/skato-iot/"
checkpoint_path = f"abfss://{ADLS_CONTAINER_NAME}@{ADLS_STORAGEACCOUNT_NAME}.dfs.core.windows.net/_checkpoint/"

spark.conf.set(
    f"fs.azure.account.key.{ADLS_STORAGEACCOUNT_NAME}.dfs.core.windows.net",
    ADLS_ACCESSKEY)

# COMMAND ----------

# DBTITLE 1,Bronze Table
@dlt.table(
  comment="""デバイスSCD30のローデータ。5秒間隔で連携されるセンサーの実測値（temperature, humidity, co2）と、計測日時、取込日時、ファイル名を記録する""",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "false" # preserves the data in the delta table if you do full refresh
  }
)
def iot_bronze():
  return (
   spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("checkpointLocation", checkpoint_path)
    .load(input_path)
    .selectExpr(
      "SystemProperties:connectionDeviceId::string AS device_name",
      "EnqueuedTimeUtc::timestamp AS event_timestamp",
      "current_timestamp() AS received_timestamp",
      "body:co2::float AS co2",
      "body:temperature::float AS temperature",
      "body:humidity::float AS humidity",
      "_metadata.file_path AS input_file_name",
      "body"
    )
  )

# COMMAND ----------

# DBTITLE 1,Silver Table
@dlt.table
@dlt.expect_or_fail("valid_id", "device_name IS NOT NULL")
@dlt.expect_or_drop("valid_temperature", "temperature IS NOT NULL")
@dlt.expect_or_drop("valid_humidity", "humidity IS NOT NULL")
@dlt.expect_or_drop("valid_co2", "co2 IS NOT NULL")
def iot_bronze_clean():
    return (
        dlt.read_stream("iot_bronze")
    )

@dlt.table(
  comment="""シルバーテーブル:データのクリーニングと初期集約を行い、デバイスごとに1分間隔のセンサー実測値（temperature, humidity, co2）の平均を計算する。""",
  table_properties={
    "quality": "silver"
  }
)
def iot_silver():
  return (
   dlt.read_stream("iot_bronze_clean")
   .withColumn('event_timestamp_jst', from_utc_timestamp(col('event_timestamp'), "JST"))
   .withColumn('received_timestamp_jst', from_utc_timestamp(col('received_timestamp'), "JST"))
   .withColumn('event_date', to_date('event_timestamp_jst'))
   .withWatermark("event_timestamp_jst", "10 minutes")
   .groupBy("device_name", window("event_timestamp_jst", "1 minute").alias("event_window"))
   .agg(
     avg("temperature").alias("temperature"),
     avg("humidity").alias("humidity"),
     avg("co2").alias("co2"),
     min("event_date").alias("event_date"),
     min("event_timestamp_jst").alias("event_timestamp_jst"),
     min("received_timestamp_jst").alias("received_timestamp_jst"),
     min("input_file_name").alias("input_file_name"),
   )
  )

# COMMAND ----------

# DBTITLE 1,Gold Table
@dlt.table(
  comment="ゴールドテーブル：デバイスごとに10分間隔でセンサー実測値（temperature, humidity, co2）の移動平均を計算する。",
  table_properties={
    "quality": "gold"
  }
)
def iot_gold_ma10min():
  return (
    dlt.read_stream("iot_silver")
    .withColumn("window_tmp", col("event_window.start").cast("timestamp"))
    .withWatermark("window_tmp", "10 minutes")
    .groupBy(window(col("window_tmp"), "10 minutes", "1 minute").alias("event_window"))
    .agg(
      avg("temperature").alias("temperature_ma10min"),
      avg("humidity").alias("humidity_ma10min"),
      avg("co2").alias("co2_ma10min")
    )
  )

# COMMAND ----------

# @dlt.table()
# def iot_per_minute():
#   iot_silver = (
#     dlt.read_stream("iot_silver")
#     .withColumn("time", col("event_window.end").cast("timestamp"))
#     .withWatermark("time", "10 minutes")
#     )
#   iot_gold = (
#     dlt.read_stream("iot_gold_ma10min")
#     .withColumn("time", col("event_window.end").cast("timestamp"))
#     .withWatermark("time", "10 minutes")
#     )
#   join_df = (
#     iot_silver.alias("s")
#     .join(
#       iot_gold.alias("g")
#       , expr("s.time = g.time")
#     )
#     .select("s.event_date", "s.time", "s.temperature", "g.temperature_ma10min", "s.humidity", "g.humidity_ma10min", "s.co2", "g.co2_ma10min", )
#   )
#   return join_df
