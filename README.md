# pysparktravis

[travisci]: https://img.shields.io/travis/com/manojdalai/pysparktravis.svg?logo=travis&logoColor=white&label=Travis%20CI
[github_release]: https://img.shields.io/github/v/release/manojdalai/pysparktravis.svg?logo=github&logoColor=white

## Pyspark
Following logic applied to python class i.e bitcointrading.py
```
- Only use clients from the United Kingdom or the Netherlands.
- Remove personal identifiable information from the first dataset, **excluding emails**. 
- Remove credit card number from the second dataset.
- Data should be joined using the **id** field. 
- Rename the columns for the easier readability to the business users:
     ----------------------------
    | Old name|     New name     |
    | ---------------------------|
    | id      | client_identifier|
    | btc_a   | bitcoin_address  |
    | cc_t    | credit_card_type |
     ---------------------------- 
```

## Generic Function

### Dataframe filter
This function is used for filtering the data set
```
    >>> for country in countries.split(','):
    >>>        if cntry == country:
    >>>           return True
    >>> return False
    
    :param countries:
    :return: no value
    :rtype: boolean
    :param cntry:
    :example
        >>> a = {"x": "x1", "y": "y1"}
        >>> b = [p, q, x, y]
        >>> rename_column(a,b)
        [p, q, x1, y1]
```
### Rename column 
This function taked the old column and return the new column which is applied in the dataframe pyspark.
``` 
    :param columndict: dictionary
    :param colnames: list
    :return: new column list
    :rtype: list
    
    >>> def getRenamedColumn(columndict, colnames):
            for key, value in columndict.items():
                oldcol, newcol = key, value
                for i, v in enumerate(colnames):
                    if oldcol in v:
                        colnames[i] = v.replace(oldcol, newcol)
        return colnames

    :example
        >>> a = {"x": "x1", "y": "y1"}
        >>> b = [p, q, x, y]
        >>> getRenamedColumn(a,b)
        [p, q, x1, y1] 
```
## Custom Logger

### Log to console
It's console handler for logging events to console. This add the log message handler to the logger
```
     >>> console_handler = logging.StreamHandler(sys.stdout)
     >>> console_handler.setFormatter(self.fmt)
     >>> return console_handler
```

### Log to file
It's console handler for logging events to log file based on the rotaion policy.
The size of the rotated files is made small so you can see the results easily
This add the log message handler to the logger
```
    >>> file_handler = RotatingFileHandler(self.logfile, maxBytes=5000, backupCount=0)
    >>> file_handler.setFormatter(self.fmt)
    >>> return file_handler
```

## Read application log in Travis CI
This function is used to check the application logs after build/deploy. This contains logs based the the format used in the custom logger. If needed you can change the log format by going to custom logger class.
```
    >>> path = "logs/application.log"
    >>> with open(path, 'r') as f:
    >>> for line in f:
    >>>    if not line.strip():
    >>>        continue
    >>>    if line:
    >>>       print(line)
```
