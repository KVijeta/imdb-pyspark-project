# Get all titles of series/movies etc. that are available in Ukrainian.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task1():
    akas_schema = t.StructType([t.StructField('titleId', t.StringType(), True),
                                t.StructField('ordering', t.IntegerType(), True),
                                t.StructField('title', t.StringType(), True),
                                t.StructField('region', t.StringType(), True),
                                t.StructField('language', t.StringType(), True),
                                t.StructField('types', t.StringType(), True),
                                t.StructField('attributes', t.StringType(), True),
                                t.StructField('isOriginalTitle', t.BooleanType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.akas.tsv.gz'
    akas_df = m.spark_session.read.csv(path,
                                       header=True,
                                       nullValue='null',
                                       schema=akas_schema, sep=r'\t')

    task1_df = akas_df.select(c.title, c.language).filter(f.col(c.language) == 'uk')
    path_to_save = './results/task1'
    write(task1_df, path_to_save)
