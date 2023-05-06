# Get 10 titles of the most popular movies/series etc. by each genre.

from read_write import write
from pyspark.sql import Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task8():
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

    transformation1_df = merger_df.withColumn(c.genres, f.explode(f.split(c.genres, ',')).alias("genre"))

    window = Window.partitionBy(c.genres).orderBy(f.desc(c.averageRating))
    transformation2_df = (transformation1_df.withColumn('rank', f.dense_rank().over(window))
                                            .filter('rank<=10')
                                            .orderBy(f.asc(c.genres)))

    window = Window.partitionBy(c.genres, c.averageRating).orderBy(f.desc(c.averageRating))
    transformation3_df = (transformation2_df.withColumn('max_votes', f.max(c.numVotes).over(window))
                                            .orderBy(f.asc(c.genres), 'rank'))
    transformation4_df = (transformation3_df.filter('numVotes == max_votes')
                          .select(f.col(c.originalTitle), f.col(c.genres), f.col(c.averageRating), f.col('rank'))
                          .orderBy(f.asc(c.genres), 'rank'))

    task8_df = transformation4_df
    path_to_save = './results/task8'
    write(task8_df, path_to_save)
