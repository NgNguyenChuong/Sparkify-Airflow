import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


def build_user_facts(spark):
    users = spark.table("iceberg.transactions.users").select("user_id")
    events = spark.table("iceberg.transactions.events")
    versions = spark.table("iceberg.transactions.song_versions").select(
        "version_id",
        "duration",
    )
    levels = spark.table("iceberg.transactions.user_levels")

    event_aggs = (
        events.alias("e")
        .join(
            versions.alias("sv"),
            F.col("e.version_id") == F.col("sv.version_id"),
            "left",
        )
        .groupBy(F.col("e.user_id"))
        .agg(
            F.count("*").alias("total_plays"),
            F.countDistinct("e.session_id").alias("total_sessions"),
            F.coalesce(F.sum("sv.duration"), F.lit(0.0)).alias(
                "total_listening_seconds"
            ),
            F.countDistinct("e.song_id").alias("distinct_songs_played"),
            F.countDistinct("e.artist_id").alias("distinct_artists_played"),
            F.min("e.ts").alias("first_seen_at"),
            F.max("e.ts").alias("last_seen_at"),
        )
    )

    latest_level = (
        levels.withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("changed_at").desc())
            ),
        )
        .filter(F.col("row_number") == 1)
        .select("user_id", F.col("new_level").alias("current_level"))
    )

    converted = (
        levels.filter(
            (F.col("new_level") == "paid") & (F.col("source") == "transition")
        )
        .select("user_id")
        .distinct()
        .withColumn("is_converted", F.lit(True))
    )

    return (
        users.alias("u")
        .join(event_aggs.alias("ea"), "user_id", "left")
        .join(latest_level.alias("cl"), "user_id", "left")
        .join(converted.alias("c"), "user_id", "left")
        .select(
            "user_id",
            F.coalesce(F.col("total_plays"), F.lit(0)).alias("total_plays"),
            F.coalesce(F.col("total_sessions"), F.lit(0)).alias("total_sessions"),
            F.coalesce(F.col("total_listening_seconds"), F.lit(0.0)).alias(
                "total_listening_seconds"
            ),
            F.coalesce(F.col("distinct_songs_played"), F.lit(0)).alias(
                "distinct_songs_played"
            ),
            F.coalesce(F.col("distinct_artists_played"), F.lit(0)).alias(
                "distinct_artists_played"
            ),
            "first_seen_at",
            "last_seen_at",
            "current_level",
            F.coalesce(F.col("is_converted"), F.lit(False)).alias("is_converted"),
        )
    )


def build_session_facts(spark):
    events = spark.table("iceberg.transactions.events")
    versions = spark.table("iceberg.transactions.song_versions").select(
        "version_id",
        "duration",
    )

    session_aggs = (
        events.alias("e")
        .join(
            versions.alias("sv"),
            F.col("e.version_id") == F.col("sv.version_id"),
            "left",
        )
        .groupBy(F.col("e.session_id"), F.col("e.user_id"))
        .agg(
            F.min("e.ts").alias("started_at"),
            F.max("e.ts").alias("ended_at"),
            F.count("*").alias("play_count"),
            F.countDistinct("e.song_id").alias("distinct_songs"),
            F.countDistinct("e.artist_id").alias("distinct_artists"),
            F.coalesce(F.sum("sv.duration"), F.lit(0.0)).alias(
                "total_listening_seconds"
            ),
        )
    )

    session_context = (
        events.withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("session_id", "user_id").orderBy(F.col("ts").asc())
            ),
        )
        .filter(F.col("row_number") == 1)
        .select("session_id", "user_id", "level", "location")
    )

    return (
        session_aggs.alias("sa")
        .join(session_context.alias("sc"), ["session_id", "user_id"], "left")
        .select(
            "session_id",
            "user_id",
            "started_at",
            "ended_at",
            (
                F.col("ended_at").cast("long") - F.col("started_at").cast("long")
            ).alias("duration_seconds"),
            "play_count",
            "distinct_songs",
            "distinct_artists",
            "total_listening_seconds",
            "level",
            "location",
        )
    )


def build_song_facts(spark):
    songs = spark.table("iceberg.transactions.songs")
    events = spark.table("iceberg.transactions.events")

    play_aggs = (
        events.filter(F.col("song_id").isNotNull())
        .groupBy("song_id")
        .agg(
            F.count("*").alias("total_plays"),
            F.countDistinct("user_id").alias("distinct_listeners"),
            F.countDistinct("session_id").alias("distinct_sessions"),
            F.countDistinct("version_id").alias("distinct_versions_played"),
            F.min("ts").alias("first_played_at"),
            F.max("ts").alias("last_played_at"),
            F.sum(F.when(F.col("level") == "paid", 1).otherwise(0)).alias(
                "paid_play_count"
            ),
        )
    )

    total_plays = F.coalesce(F.col("pa.total_plays"), F.lit(0))
    return (
        songs.alias("s")
        .join(play_aggs.alias("pa"), F.col("s.song_id") == F.col("pa.song_id"), "left")
        .select(
            F.col("s.song_id").alias("song_id"),
            F.col("s.title").alias("title"),
            F.col("s.artist_id").alias("artist_id"),
            total_plays.alias("total_plays"),
            F.coalesce(F.col("pa.distinct_listeners"), F.lit(0)).alias(
                "distinct_listeners"
            ),
            F.coalesce(F.col("pa.distinct_sessions"), F.lit(0)).alias(
                "distinct_sessions"
            ),
            F.coalesce(F.col("pa.distinct_versions_played"), F.lit(0)).alias(
                "distinct_versions_played"
            ),
            F.col("pa.first_played_at").alias("first_played_at"),
            F.col("pa.last_played_at").alias("last_played_at"),
            F.when(total_plays == 0, F.lit(0.0))
            .otherwise(F.col("pa.paid_play_count").cast("double") / total_plays)
            .alias("paid_play_share"),
        )
    )


def build_artist_facts(spark):
    artists = spark.table("iceberg.transactions.artists")
    events = spark.table("iceberg.transactions.events")

    play_aggs = (
        events.filter(F.col("artist_id").isNotNull())
        .groupBy("artist_id")
        .agg(
            F.count("*").alias("total_plays"),
            F.countDistinct("user_id").alias("distinct_listeners"),
            F.countDistinct("song_id").alias("distinct_songs_played"),
            F.countDistinct("session_id").alias("distinct_sessions"),
            F.min("ts").alias("first_played_at"),
            F.max("ts").alias("last_played_at"),
            F.sum(F.when(F.col("level") == "paid", 1).otherwise(0)).alias(
                "paid_play_count"
            ),
        )
    )

    total_plays = F.coalesce(F.col("pa.total_plays"), F.lit(0))
    return (
        artists.alias("a")
        .join(
            play_aggs.alias("pa"),
            F.col("a.artist_id") == F.col("pa.artist_id"),
            "left",
        )
        .select(
            F.col("a.artist_id").alias("artist_id"),
            F.col("a.artist_name").alias("artist_name"),
            total_plays.alias("total_plays"),
            F.coalesce(F.col("pa.distinct_listeners"), F.lit(0)).alias(
                "distinct_listeners"
            ),
            F.coalesce(F.col("pa.distinct_songs_played"), F.lit(0)).alias(
                "distinct_songs_played"
            ),
            F.coalesce(F.col("pa.distinct_sessions"), F.lit(0)).alias(
                "distinct_sessions"
            ),
            F.col("pa.first_played_at").alias("first_played_at"),
            F.col("pa.last_played_at").alias("last_played_at"),
            F.when(total_plays == 0, F.lit(0.0))
            .otherwise(F.col("pa.paid_play_count").cast("double") / total_plays)
            .alias("paid_play_share"),
        )
    )


BUILDERS = {
    "artist_facts": build_artist_facts,
    "session_facts": build_session_facts,
    "song_facts": build_song_facts,
    "user_facts": build_user_facts,
}

args = getResolvedOptions(sys.argv, ["JOB_NAME", "config"])
config = json.loads(args["config"])
table_name = config["table"]

if table_name not in BUILDERS:
    raise ValueError(f"Unsupported analytics table: {table_name}")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

result_df = BUILDERS[table_name](spark)
target_table = f"iceberg.analytics.{table_name}"

(
    result_df.writeTo(target_table)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print(f"Completed Iceberg snapshot replacement for {table_name}")
job.commit()
