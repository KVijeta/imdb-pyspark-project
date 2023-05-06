# Get names of people, corresponding movies/series and characters they
# played in those films.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task4():
    principals_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                      t.StructField('ordering', t.IntegerType(), True),
                                      t.StructField('nconst', t.StringType(), True),
                                      t.StructField('category', t.StringType(), True),
                                      t.StructField('job', t.StringType(), True),
                                      t.StructField('characters', t.StringType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.principals.tsv.gz'
    principals_df = m.spark_session.read.csv(path,
                                             header=True,
                                             nullValue='null',
                                             schema=principals_schema, sep=r'\t')

    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                        t.StructField('titleType', t.StringType(), True),
                                        t.StructField('primaryTitle', t.StringType(), True),
                                        t.StructField('originalTitle', t.StringType(), True),
                                        t.StructField('isAdult', t.IntegerType(), True),
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

    name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), True),
                                       t.StructField('primaryName', t.StringType(), True),
                                       t.StructField('birthYear', t.IntegerType(), True),
                                       t.StructField('deathYear', t.StringType(), True),
                                       t.StructField('primaryProfession', t.StringType(), True),
                                       t.StructField('knownForTitles', t.StringType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/name.basics.tsv.gz'
    name_basics_df = m.spark_session.read.csv(path,
                                              header=True,
                                              nullValue='null',
                                              dateFormat='yyyy',
                                              schema=name_basics_schema, sep=r'\t')

    merger_df = (principals_df.join(name_basics_df, on='nconst', how='left')
                              .join(title_basics_df, on='tconst', how='left'))

    task4_df = (merger_df.select(c.category, c.characters, c.originalTitle, c.primaryName)
                         .filter(f.col(c.category).isin('actor', 'actress')))
    path_to_save = './results/task4'
    write(task4_df, path_to_save)
