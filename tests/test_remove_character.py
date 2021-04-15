import pytest
from functions import remove_non_word_characters
from chispa import *
import pyspark.sql.functions as F
def test_remove_non_word_characters(spark):
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")