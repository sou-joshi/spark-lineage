# Sample PySpark job (for demo narrative)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("policy_daily").enableHiveSupport().getOrCreate()

policy = spark.table("default.policy_currt")
cust = spark.table("default.customer_dim")

df = (policy
      .join(cust, on="customer_id", how="inner")
      .select(
          col("policy_id"),
          col("customer_email"),
          (col("premium")/100).alias("risk_score"),
      )
      .filter(col("risk_score") > 0)
)

df.write.mode("overwrite").saveAsTable("default.mart_policy_daily")
