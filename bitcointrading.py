"""

"""
import sys
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from userdefinedfunction import functions
from pyspark.sql import functions as F
from customlogger import loghandler


class KommatiPara:
    """

    """
    currenttime = datetime.now().strftime('%Y%m%d%H%M%S')

    # INITIALIZING LOGGING FRAMEWORK
    log = loghandler.getLogger("ABM AMRO")
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
        """load data sets such as dataset 1 and dataset 3

        :param filename: input file nema (csv).
        :return: dataframe with header
        """
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
        df1 = df1.drop('email')
        df1_1 = df1.filter(functions.isCountryMatchedUDF(F.lit(countries), df1.country))
    except:
        log.error("There is an exception in filtering data frame")
    df1_1.printSchema()
    df1_1.show()

    # CREATING DATA FRAME FOR DATASET 2
    log.info("Loading 2nd data set")
    df2 = load_dataframe(spark, sys.argv[2])
    df2_1 = df2.drop('cc_n')
    df2_1.printSchema()
    df2_1.show()

    # DATAFRAME JOINING
    log.info("Joining 2 data frame. Performing Left join")
    join_df = df1_1.join(df2_1, 'id', 'left')
    join_df.printSchema()
    join_df.show()

    log.info("Fetching column name from the data frame")
    colnames = join_df.columns

    log.info("Old column of the data frame %s" % (colnames))
    log.info("Old and new column mapping")
    columndict = {"btc_a": "bitcoin_address", "cc_t": "credit_card_type", "id": "client_identifier"}

    # RENAME COLUMNS OF THE DATAFRAME
    log.info("Renaming column of data frame")
    try:
        newcolumn = functions.getRenamedColumn(columndict, colnames)
        log.info("New column of the data frame %s" % (newcolumn))
        rename_df = join_df.toDF(*newcolumn)
    except:
        log.error("There is an exception in renaming column")

    rename_df.printSchema()
    rename_df.show()

    # log.info("Renaming the columns")
    # rename_df = join_df.withColumnRenamed('id', 'client_identifier') \
    #     .withColumnRenamed('btc_a', 'bitcoin_address') \
    #     .withColumnRenamed('cc_t', 'credit_card_type')
    # rename_df.show()

    log.info("Writing data as CSV to location 'client_data' ")
    rename_df.write \
        .format('csv') \
        .option('header',True) \
        .mode('overwrite') \
        .option('sep',',') \
        .save(output)

