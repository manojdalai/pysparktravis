# pysparktravis

[travisci]: https://img.shields.io/travis/com/manojdalai/pysparktravis.svg?logo=travis&logoColor=white&label=Travis%20CI
[github_release]: https://img.shields.io/github/v/release/manojdalai/pysparktravis.svg?logo=github&logoColor=white
[![Travis CI][travisci]](https://travis-ci.com/manojdalai/pysparktravis)
[![Generic badge](https://img.shields.io/badge/pyspark-2.4.6-<COLOR>.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/python-3.7.x-<COLOR>.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/Hadoop-2.7-<COLOR>.svg)](https://shields.io/)
[![GitHub Release][github_release]](https://github.com/manojdalai/pysparktravis/releases/)
![Maintaner](https://img.shields.io/badge/maintainer-ManojDalai-blue)

## Environment Details
```
    Python 3.7.x
    Pyspark 2.4.x
    Git
    Travis-CI
    Hadoop 2.7
```

## How to use?
```
    - Clone this git repository to your local repo. 
    - Fullfil above environment details.
    - Configure project locally in editor of your choice. In my ase it is PyCharm.
    - Integratie your Git remote repo with Travis CI.
    - Push the code Git and see automate build and continuous integration in Travis CI
    - Merge the code after build is successful.
    
```
## Pyspark
Following logic applied to python class i.e bitcointrading.py
```
    - Only use clients from the United Kingdom or the Netherlands.
    - Remove personal identifiable information from the first dataset, **excluding emails**. 
    - Remove credit card number from the second dataset.
    - Data should be joined using the **id** field. 
    - Limited exception handling.
    - Custom logger.
    - Generic function.
    - PII data handle
    - Rename the columns for the easier readability to the business users:
        +---------+------------------+
        | Old name|     New name     |
        | --------+------------------|
        | id      | client_identifier|
        | btc_a   | bitcoin_address  |
        | cc_t    | credit_card_type |
        +---------+------------------+
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

## Github & Travis-CI Integration

### Build at Travis
```Build at Travis```
![](images/build_at_travis.png)

### Github Check API on Travis-CI
```Push to remote branch```
![](images/push_to_remote_branch.png)

```Build branch after pullrequest```
![](images/build_branch_after_pull_request.png)

```Build pass by travis```
![](images/build_pass_by_travis.png)

```Approve merge```
![](images/approve_merge.png)

```Merge branch to maater```
![](images/merge_branch_to_maater.png)

## Output/Result
In this result Emails are masked and removed the last_name because I believe those are personal or PII data as per the use case.
Columns are renames from old columns to new columns such as client_identifier, bitcoin_address, credit_card_type
``` 
    +-----------------+----------+--------------------+--------------+--------------------+--------------------+
    |client_identifier|first_name|               email|       country|     bitcoin_address|    credit_card_type|
    +-----------------+----------+--------------------+--------------+--------------------+--------------------+
    |               18|   Richard|r***h@odnoklassni...|United Kingdom|1ErM8yuF3ytzzxLy1...|      china-unionpay|
    |               32|    Wallis|   w***v@t-online.de|United Kingdom|12sxmYnPcADAXw1Yk...|             maestro|
    |               33|   Saundra| s***w@blinklist.com|United Kingdom|1GZ7QB7GUFSWnkBHm...|          mastercard|
    |               34|     Ewart|  e***x@usatoday.com|United Kingdom|12o8zrHx6snCPbtko...|       visa-electron|
    |               36|    Daniel|     d***z@tmall.com|   Netherlands|15X53Z9B9jUNrvFpb...|diners-club-inter...|
    |               62|      Boyd|      b***p@wisc.edu|   Netherlands|16qpYVt6YAAx4JYjz...|                 jcb|
    |               67|     Lorry|l***u@technorati.com|United Kingdom|12ya1ED93ApPBQRSC...|            bankcard|
    |               70|  Freedman| f***x@bloglines.com|United Kingdom|1F8zXTEaf4AFpztMN...|          mastercard|
    |               91|      Audy| a***i@webeden.co.uk|United Kingdom|19MZSy1H8S4SaXsmS...|diners-club-carte...|
    |              105|     Lacie|l***w@telegraph.c...|   Netherlands|13j6FKzrLgumLUqeY...|                 jcb|
    |              108|   Rhianna|       r***z@cdc.gov|   Netherlands|1RcsodKknm8thkCL6...|             maestro|
    |              109|    Marnia|      m***0@uiuc.edu|   Netherlands|1HxV2jkyM3PXbsH4q...|                 jcb|
    |              110|     Rhody|     r***1@tmall.com|   Netherlands|15cRJ4mzZd4Vgd33x...|      china-unionpay|
    |              124|       Wyn| w***f@geocities.com|   Netherlands|1CB7AdhTFBXmxuABm...|             maestro|
    |              128|      Vito|      v***j@ning.com|   Netherlands|16DbYq1KR8DVSQu5E...| diners-club-enroute|
    |              165|     Hilda|h***k@altervista.org|   Netherlands|1KgbP1KXt5xs2sBLu...|                 jcb|
    |              177|   Alfredo|  a***w@geocities.jp|United Kingdom|1AJzqEgbbFh2TNLFZ...|     americanexpress|
    |              189|    Eduard|       e***8@ovh.net|   Netherlands|1PktCHyic9G4aZu15...|                 jcb|
    |              194|     Irena|      i***d@live.com|   Netherlands|12KTfvJwTfnJn3dFt...|                 jcb|
    |              197|    Penrod|       p***g@ask.com|United Kingdom|1Q6UV84patYXfzEdA...| diners-club-enroute|
    +-----------------+----------+--------------------+--------------+--------------------+--------------------+    

```