"""
Latitude Lineage Demo Job (enterprise-style)
Flow:
  S3 RAW (CSV) -> S3 CONFORMED (Parquet) -> Oracle ODS -> Oracle STG -> Oracle MART

Entities:
  RAW:
    s3://telco-raw/gps_pings/dt=2026-02-15/gps_pings.csv
    s3://telco-raw/device_dim/dt=2026-02-15/device_dim.csv

  CONFORMED:
    s3://telco-conformed/gps_pings_parquet/dt=2026-02-15/
    s3://telco-conformed/device_dim_parquet/dt=2026-02-15/

  ORACLE:
    ORACLE.ODS_GPS_PING
    ORACLE.ODS_DEVICE_DIM
    ORACLE.STG_LATITUDE_SESSION
    ORACLE.MART_LATITUDE_DAILY
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, when, sha2, concat_ws, count, avg, max as fmax, min as fmin

spark = (SparkSession.builder
         .appName("latitude_lineage_demo")
         .getOrCreate())

run_dt = "2026-02-15"

# ------------------------
# 1) RAW -> CONFORMED
# ------------------------
raw_pings = (spark.read
             .option("header", "true")
             .csv(f"s3://telco-raw/gps_pings/dt={run_dt}/gps_pings.csv"))

raw_device = (spark.read
              .option("header", "true")
              .csv(f"s3://telco-raw/device_dim/dt={run_dt}/device_dim.csv"))

# Clean + type normalize + derive columns
pings_conf = (
    raw_pings
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("lat_number", col("lat_number").cast("double"))
    .withColumn("lon_number", col("lon_number").cast("double"))
    .withColumn("event_date", date_format(col("event_ts"), "yyyy-MM-dd"))
    .withColumn("lat_band",
        when(col("lat_number") >= 0, "NORTH").otherwise("SOUTH"))
    .filter(col("lat_number").isNotNull() & col("lon_number").isNotNull())
)

device_conf = (
    raw_device
    .withColumn("device_key", sha2(concat_ws("::", col("device_id"), col("msisdn")), 256))
    .select("device_key", "device_id", "msisdn", "device_type", "os_version")
)

pings_conf_path = f"s3://telco-conformed/gps_pings_parquet/dt={run_dt}/"
device_conf_path = f"s3://telco-conformed/device_dim_parquet/dt={run_dt}/"

(pings_conf.write.mode("overwrite").parquet(pings_conf_path))
(device_conf.write.mode("overwrite").parquet(device_conf_path))

# ------------------------
# 2) CONFORMED -> ORACLE ODS (JDBC)
# ------------------------
jdbc_url = "jdbc:oracle:thin:@//oracle.host:1521/ORCL"
jdbc_props = {"user": "svc_user", "password": "****", "driver": "oracle.jdbc.OracleDriver"}

ods_pings = spark.read.parquet(pings_conf_path)
ods_device = spark.read.parquet(device_conf_path)

# Simulate ODS landing: uppercase columns + batch id
ods_pings_out = (ods_pings
                 .withColumnRenamed("event_ts", "EVENT_TS")
                 .withColumnRenamed("lat_number", "LAT_NUMBER")
                 .withColumnRenamed("lon_number", "LON_NUMBER")
                 .withColumnRenamed("event_date", "EVENT_DATE")
                 .withColumnRenamed("lat_band", "LAT_BAND")
                 .withColumnRenamed("device_id", "DEVICE_ID")
                 .withColumn("BATCH_ID", col("EVENT_DATE")))

ods_device_out = (ods_device
                  .withColumnRenamed("device_key", "DEVICE_KEY")
                  .withColumnRenamed("device_id", "DEVICE_ID")
                  .withColumnRenamed("msisdn", "MSISDN")
                  .withColumnRenamed("device_type", "DEVICE_TYPE")
                  .withColumnRenamed("os_version", "OS_VERSION"))

(ods_pings_out.write.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.ODS_GPS_PING", **jdbc_props).mode("append").save())
(ods_device_out.write.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.ODS_DEVICE_DIM", **jdbc_props).mode("append").save())

# ------------------------
# 3) ODS -> STAGING
# ------------------------
ods_p = spark.read.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.ODS_GPS_PING", **jdbc_props).load()
ods_d = spark.read.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.ODS_DEVICE_DIM", **jdbc_props).load()

# Enrich pings with device_key + PII masking
stg = (
    ods_p.join(ods_d, on="DEVICE_ID", how="left")
    .withColumn("MSISDN_HASH", sha2(col("MSISDN"), 256))
    .select("DEVICE_ID", "DEVICE_KEY", "EVENT_TS", "EVENT_DATE", "LAT_NUMBER", "LON_NUMBER", "LAT_BAND", "DEVICE_TYPE", "MSISDN_HASH")
)

(stg.write.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.STG_LATITUDE_SESSION", **jdbc_props).mode("overwrite").save())

# ------------------------
# 4) STAGING -> MART
# ------------------------
stg_in = spark.read.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.STG_LATITUDE_SESSION", **jdbc_props).load()

mart = (
    stg_in.groupBy("EVENT_DATE", "LAT_BAND")
    .agg(
        count("*").alias("PING_COUNT"),
        avg("LAT_NUMBER").alias("AVG_LAT"),
        fmin("LAT_NUMBER").alias("MIN_LAT"),
        fmax("LAT_NUMBER").alias("MAX_LAT")
    )
)

(mart.write.format("jdbc").options(url=jdbc_url, dbtable="ORACLE.MART_LATITUDE_DAILY", **jdbc_props).mode("overwrite").save())

spark.stop()
