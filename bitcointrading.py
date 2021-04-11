import sys
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from userdefinedfunction import functions
from pyspark.sql import functions as F
from customlogger import getLogger


class KommatiPara:

    currenttime = datetime.now().strftime('%Y%m%d%H%M%S')

    # INITIALIZING LOGGING FRAMEWORK
    log = getLogger("ABM AMRO")
    log.info("Starting pyspark application...")
    log.info("Current time %s" % currenttime)
    log.info("Input filepath for dataset 1 %s" % (sys.argv[1]))
    log.info("Input filepath for dataset 2 %s" % (sys.argv[2]))
    log.info("Country list %s" % (sys.argv[3]))
    log.info("Output directory %s" % (sys.argv[4]))
    output = sys.argv[4]+'/' + currenttime

    # SCHEMA DEFINATION
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('country', StringType(), True)
    ])

    # CREATING SPARK SESSION
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('ABN AMRO') \
        .getOrCreate()

    def load_dataframe(spark, filename):
        raw_data = spark.read \
            .format('csv') \
            .option('header', 'true') \
            .load(filename)
        return raw_data

    # CREATING DATA FRAME FOR DATASET 1
    log.info("Loading 1st data set")
    df1 = load_dataframe(spark, sys.argv[1])
    countries = sys.argv[3]
    # FILTER DATA FRAME
    try:
        df1_1 = df1.filter(functions.isCountryMatchedUDF(F.lit(countries), df1.country))
    except:
        log.error("There is an exception in filtering data frame")

    df1_1.printSchema()
    df1_1.show()



