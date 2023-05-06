# Get titles of all movies that last more than 2 hours.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task3():
    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                        t.StructField('titleType', t.StringType(), True),
                                        t.StructField('primaryTitle', t.StringType(), True),
                                        t.StructField('originalTitle', t.StringType(), True),
                                        t.StructField('isAdult', t.BooleanType(), True),
                                        t.StructField('startYear', t.StringType(), True),
                                        t.StructField('endYear', t.StringType(), True),
                                        t.StructField('runtimeMinutes', t.IntegerType(), True),
                                        t.StructField('genres', t.StringType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.basics.tsv.gz'
    title_basics_df = m.spark_session.read.csv(path,
                                               header=True,
                                               nullValue='null',
                                               dateFormat='yyyy',
                                               schema=title_basics_schema, sep=r'\t')

    task3_df = (title_basics_df.select(c.titleType, c.primaryTitle, c.originalTitle, c.runtimeMinutes)
                               .filter((f.col(c.titleType) == 'movie') & (f.col(c.runtimeMinutes) > 120)))
    path_to_save = './results/task3'
    write(task3_df, path_to_save)
