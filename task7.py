# Get 10 titles of the most popular movies/series etc. by each decade.

from read_write import write
from pyspark.sql import Window
from pyspark.sql.functions import floor
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task7():
    title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                        t.StructField('titleType', t.StringType(), True),
                                        t.StructField('primaryTitle', t.StringType(), True),
                                        t.StructField('originalTitle', t.StringType(), True),
                                        t.StructField('isAdult', t.IntegerType(), True),
                                        t.StructField('startYear', t.IntegerType(), True),
                                        t.StructField('endYear', t.StringType(), True),
                                        t.StructField('runtimeMinutes', t.IntegerType(), True),
                                        t.StructField('genres', t.StringType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.basics.tsv.gz'
    title_basics_df = m.spark_session.read.csv(path,
                                               header=True,
                                               nullValue='null',
                                               schema=title_basics_schema, sep=r'\t')

    ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                   t.StructField('averageRating', t.DoubleType(), True),
                                   t.StructField('numVotes', t.IntegerType(), True)])
    path = 'E:/Workspace_Python/PycharmProjects/pythonProject/imdb-pyspark-project/imdb-data/title.ratings.tsv.gz'
    ratings_df = m.spark_session.read.csv(path,
                                          header=True,
                                          nullValue='null',
                                          schema=ratings_schema, sep=r'\t')

    merger_df = title_basics_df.join(ratings_df, on=c.tconst, how='left')

    transformation1_df = merger_df.withColumn('decade', floor(f.col(c.startYear) / 10) * 10)

    window = Window.partitionBy('decade').orderBy(f.desc(c.averageRating))
    transformation2_df = (transformation1_df.withColumn('rank', f.dense_rank().over(window))
                          .filter('rank<=10').orderBy(f.desc('decade')))

    window = Window.partitionBy('decade', c.averageRating).orderBy(f.desc(c.averageRating))
    transformation3_df = (transformation2_df.withColumn('max_votes', f.max(c.numVotes).over(window))
                          .orderBy(f.desc('decade')))
    transformation4_df = (transformation3_df.filter('numVotes == max_votes')
                          .select(f.col(c.originalTitle), f.col('decade'), f.col(c.averageRating), f.col('rank'))
                          .orderBy(f.desc('decade'), 'rank'))

    task7_df = transformation4_df
    path_to_save = './results/task7'
    write(task7_df, path_to_save)
