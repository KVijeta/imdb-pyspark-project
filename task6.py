# Get information about how many episodes in each TV Series. Get the top
# 50 of them starting from the TV Series with the biggest quantity of
# episodes.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task6():
    episode_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                   t.StructField('parentTconst', t.StringType(), True),
                                   t.StructField('seasonNumber', t.IntegerType(), True),
                                   t.StructField('episodeNumber', t.IntegerType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.episode.tsv.gz'
    episode_df = m.spark_session.read.csv(path,
                                          header=True,
                                          nullValue='null',
                                          schema=episode_schema, sep=r'\t')

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

    merger_df = episode_df.join(title_basics_df, on=c.tconst, how='left')

    transformation1_df = merger_df.withColumn('sum_episode', f.col(c.seasonNumber) * f.col(c.episodeNumber))
    transformation2_df = (transformation1_df.select(c.originalTitle, 'sum_episode')
                          .orderBy('sum_episode', ascending=False))

    task6_df = transformation2_df.limit(50)
    path_to_save = './results/task6'
    write(task6_df, path_to_save)
