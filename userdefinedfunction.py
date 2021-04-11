from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

class functions:
    def isCountryMatched(countries, cntry):
        for country in countries.split(','):
            if cntry == country:
                return True
        return False

    isCountryMatchedUDF = udf(isCountryMatched, BooleanType())

