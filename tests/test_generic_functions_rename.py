import pytest
from userdefinedfunction import *
import pyspark.sql.functions as F
from chispa.dataframe_comparer import *

def test_filter_countries(spark):

    source_data = [
        ("1","United States",),
        ("2","France",),
        ("3","United Kingdom",),
        ("4","Netherlands",)
    ]

    source_df = spark.createDataFrame(source_data, ["id","cnt"])
    columndict = {"cnt": "country", "id": "client_identifier"}

    newcolumn = functions.getRenamedColumn(columndict, source_df.columns)
    rename_df = source_df.toDF(*newcolumn)

    target_data = [
        ("1", "United States",),
        ("2", "France",),
        ("3", "United Kingdom",),
        ("4", "Netherlands",)
    ]

    target_df = spark.createDataFrame(target_data, ["client_identifier", "country"])
    assert_df_equality(rename_df, target_df)