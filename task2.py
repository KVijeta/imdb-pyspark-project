# Get the list of peopleʼs names, who were born in the 19th century.

from read_write import write
import pyspark.sql.types as t
import pyspark.sql.functions as f
import main as m
import columns as c


def task2():
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

    task2_df = (name_basics_df.select(c.primaryName, c.birthYear)
                              .filter((f.col(c.birthYear) > 1800) & (f.col(c.birthYear) <= 1900)))
    path_to_save = './results/task2'
    write(task2_df, path_to_save)
