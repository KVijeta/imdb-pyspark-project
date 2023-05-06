# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task5():
    ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                   t.StructField('averageRating', t.DoubleType(), True),
                                   t.StructField('numVotes', t.IntegerType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.ratings.tsv.gz'
    ratings_df = m.spark_session.read.csv(path,
                                          header=True,
                                          nullValue='null',
                                          schema=ratings_schema, sep=r'\t')

    akas_schema = t.StructType([t.StructField('titleId', t.StringType(), True),
                                t.StructField('ordering', t.IntegerType(), True),
                                t.StructField('title', t.StringType(), True),
                                t.StructField('region', t.StringType(), True),
                                t.StructField('language', t.StringType(), True),
                                t.StructField('types', t.StringType(), True),
                                t.StructField('attributes', t.StringType(), True),
                                t.StructField('isOriginalTitle', t.IntegerType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.akas.tsv.gz'
    akas_df = m.spark_session.read.csv(path,
                                       header=True,
                                       nullValue='null',
                                       schema=akas_schema, sep=r'\t')

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
                                               schema=title_basics_schema, sep=r'\t')

    merger1_df = akas_df.join(title_basics_df, on=title_basics_df[c.tconst] == akas_df[c.titleId], how='left')
    merger2_df = merger1_df.join(ratings_df, on=c.tconst, how='left')

    transformation1_df = merger2_df.filter(f.col(c.isAdult) == 1)
    transformation2_df = transformation1_df.select(c.title, c.region, c.numVotes).orderBy(c.numVotes, ascending=False)
    transformation3_df = transformation2_df.groupBy(c.region).count().orderBy('count', ascending=False)

    task5_df = transformation3_df.limit(100)
    path_to_save = './results/task5'
    write(task5_df, path_to_save)
