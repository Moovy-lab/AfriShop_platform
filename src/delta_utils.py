from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CURATED = "/warehouse/curated"


def dedup_latest(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert(spark: SparkSession, df: DataFrame, table: str, key: str) -> None:
    path = f"{CURATED}/{table}"
    df = dedup_latest(df, key)
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        return
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")  # ignore les données plus anciennes
        .whenNotMatchedInsertAll()
        .execute()
    )