from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

class functions:

    # GENERIC FUNCTION TO FILTER DATA FRAME
    def isCountryMatched(countries, cntry):
        for country in countries.split(','):
            if cntry == country:
                return True
        return False

    isCountryMatchedUDF = udf(isCountryMatched, BooleanType())

    # GENERIC FUNCTION TO RENAME COLUMNS
    def remane_column(columndict, colnames ):
        for key, value in columndict.items():
            oldcol, newcol = key, value
            for i, v in enumerate(colnames):
                if oldcol in v:
                    colnames[i] = v.replace(oldcol, newcol)
        return colnames
