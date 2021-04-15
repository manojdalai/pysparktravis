import pytest
from userdefinedfunction import *
import pyspark.sql.functions as F
from chispa.dataframe_comparer import *

def test_filter_countries(spark):

    source_data = [
        ("United States",),
        ("France",),
        ("United Kingdom",),
        ("Netherlands",)
    ]

    source_df = spark.createDataFrame(source_data, ["countries"])
    isCountryMatchedUDF = udf(functions.isCountryMatched, BooleanType())
    conutry = "France,Netherlands,"
    actual_df = source_df.filter(isCountryMatchedUDF(F.lit(conutry), source_df.countries))

    expected_data = [
        ("France",),
        ("Netherlands",)
    ]

    expected_df = spark.createDataFrame(expected_data, ["countries"])
    assert_df_equality(actual_df, expected_df)