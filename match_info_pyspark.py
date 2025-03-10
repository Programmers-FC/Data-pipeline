import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, col
from pyspark.sql.types import IntegerType,DateType

# 날짜 파라미터 받기
if len(sys.argv) != 2:
    print("Usage: spark-submit <script.py> <processing_date>")
    sys.exit(1)

processing_date = sys.argv[1]
print(f"Processing date: {processing_date}")

# Spark 설정
conf = SparkConf()
conf.set("spark.driver.memory", "1g") # 드라이버 ram 용량 1GB
conf.set("spark.executor.memory", "1g") # Executor ram 용량 1GB
conf.set("spark.driver.cores","1") # driver CPU Core 1개
conf.set("park.executor.cores", "1") # Executor core 1개
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain") #AWS 서비스간 인증 관련 패키지

# SparkSession 생성
spark = SparkSession.builder \
    .master("local[1]") \
    .appName('json to parquet') \
    .config(conf=conf) \
    .getOrCreate()

# S3 경로 날짜 파라미터 반영
json_path = f"s3a://de5-finalproj-team2/raw_data/json/match_data/{processing_date}/*.json"
trend_output_path = f"s3a://de5-finalproj-team2/trend_analysis/match_data/{processing_date}/"
analysis_output_path = f"s3a://de5-finalproj-team2/analytics/match_data/{processing_date}/"

print(f"Reading json file from: {json_path}")
df = spark.read.json(json_path)

# 데이터 가공
df_match_info = df.withColumn("player", explode("player_info")).drop("player_info")

df_match_info_trend = df_match_info.select(
    col("matchId").alias("match_id"),
    col("player.spId").alias("spid"),
    col("seasonId").alias("season_id"),
    col("nickname").alias("gamer_nickname"),
    col("player.spPosition").alias("position"),
    col("ouid"),
    col("player.spGrade").alias("spgrade"),
    col("matchResult").alias("matchresult"),
    col("matchDate").alias("match_date")
)

df_match_info_trend = df_match_info_trend.withColumn("spid", col("spid").cast(IntegerType())) \
                                        .withColumn("season_id", col("season_id").cast(IntegerType())) \
                                        .withColumn("position", col("position").cast(IntegerType())) \
                                        .withColumn("spgrade", col("spgrade").cast(IntegerType()))\
                                        .withColumn("match_date", col("match_date").cast(DateType()))

# 저장 - Trend Analysis
print("Writing trend_analysis parquet")
df_match_info_trend.write.format("parquet").mode("overwrite").save(trend_output_path)



# 저장 - Analytics
df_match_info_analysis = df_match_info_trend.select(
    col("match_id"),
    col("spid"),
    col("season_id"),
    col("gamer_nickname"),
    col("position"),
    col("spgrade"),
    col("matchresult")
)

print("Writing analytics parquet")
df_match_info_analysis.write.format("parquet").mode("overwrite").save(analysis_output_path)

spark.stop()
