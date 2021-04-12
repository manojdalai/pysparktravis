from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

class functions:
    # GENERIC FUNCTION TO FILTER DATA FRAME
    def isCountryMatched(countries, cntry):
        """
        :param countries:
        :param cntry:
        :return: no value
        :rtype: boolean
        :example
        >>> a = {"x": "x1", "y": "y1"}
        >>> b = [p, q, x, y]
        >>> rename_column(a,b)
        [p, q, x1, y1]
        """
        for country in countries.split(','):
            if cntry == country:
                return True
        return False

    isCountryMatchedUDF = udf(isCountryMatched, BooleanType())

    # GENERIC FUNCTION TO RENAME COLUMNS
    def getRenamedColumn(columndict, colnames):
        """Return renamed column

        The value of the colmndict is stored the column mapping between old and new column.
        The value of the colnames is stored the columns of the data frame.
        The function retun the new column list by replacing the selected or all old columns as per need.

        :param columndict: dictionary
        :param colnames: list
        :return: new column list
        :rtype: list
        :example
        >>> a = {"x": "x1", "y": "y1"}
        >>> b = [p, q, x, y]
        >>> getRenamedColumn(a,b)
        [p, q, x1, y1]
        """
        for key, value in columndict.items():
            oldcol, newcol = key, value
            for i, v in enumerate(colnames):
                if oldcol in v:
                    colnames[i] = v.replace(oldcol, newcol)
        return colnames
