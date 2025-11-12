from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import os

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB   = os.getenv("PG_DB",   "quotes_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "admin")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED = BASE_DIR / "data" / "processed"

AUTHORS_PATH = str(PROCESSED / "authors")
TAGS_PATH    = str(PROCESSED / "tags")
QUOTES_PATH  = str(PROCESSED / "quotes")

spark = (
    SparkSession.builder
        .appName("LoadToPostgres")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def read_csv_folder(path: str):
    return (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
    )

def safe_read_table_names_only(table_name: str, col_name: str = "name"):
    """Return single-column DF (name) for an existing table, or empty DF if it doesn't exist."""
    try:
        df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)
        if col_name not in df.columns:
            return spark.createDataFrame([], schema=StructType([StructField(col_name, StringType(), True)]))
        return df.select(col_name).dropna().dropDuplicates([col_name])
    except Exception:
        return spark.createDataFrame([], schema=StructType([StructField(col_name, StringType(), True)]))

# -- AUTHORS --
df_authors_src = read_csv_folder(AUTHORS_PATH)
if "name" not in df_authors_src.columns:
    raise RuntimeError("Authors CSV must include a 'name' column.")

df_authors = df_authors_src.select(col("name").cast("string")).dropna().dropDuplicates(["name"])
existing_authors = safe_read_table_names_only("public.authors", "name")
df_authors_new = df_authors.join(existing_authors, on="name", how="left_anti")

new_authors_count = df_authors_new.count()
if new_authors_count > 0:
    df_authors_new.write.jdbc(url=JDBC_URL, table="public.authors", mode="append", properties=JDBC_PROPS)
print(f"Authors inserted: {new_authors_count}")

# -- TAGS --
df_tags_src = read_csv_folder(TAGS_PATH)
if "name" not in df_tags_src.columns:
    raise RuntimeError("Tags CSV must include a 'name' column.")

df_tags = df_tags_src.select(col("name").cast("string")).dropna().dropDuplicates(["name"])
existing_tags = safe_read_table_names_only("public.tags", "name")
df_tags_new = df_tags.join(existing_tags, on="name", how="left_anti")

new_tags_count = df_tags_new.count()
if new_tags_count > 0:
    df_tags_new.write.jdbc(url=JDBC_URL, table="public.tags", mode="append", properties=JDBC_PROPS)
print(f"Tags inserted: {new_tags_count}")

# -- QUOTES --
df_quotes_src = read_csv_folder(QUOTES_PATH)

if "quote_text" not in df_quotes_src.columns:
    raise RuntimeError("Quotes CSV must contain 'quote_text'.")
if "author" not in df_quotes_src.columns:
    raise RuntimeError("Quotes CSV must contain 'author' (the author name).")

dfq = df_quotes_src

if "page_number" in dfq.columns:
    dfq = dfq.withColumn("page_number", col("page_number").cast("int"))
if "scraped_at" in dfq.columns:
    dfq = dfq.withColumn("scraped_at", to_timestamp(col("scraped_at")))

select_cols = ["quote_text", "author"]
if "page_number" in dfq.columns:
    select_cols.append("page_number")
if "scraped_at" in dfq.columns:
    select_cols.append("scraped_at")

df_quotes = dfq.select(*select_cols)

quotes_stage_count = df_quotes.count()
if quotes_stage_count > 0:
    df_quotes.write.jdbc(url=JDBC_URL, table="public.quotes_stage", mode="append", properties=JDBC_PROPS)
print(f"Quotes staged (quotes_stage): {quotes_stage_count}")

spark.stop()
