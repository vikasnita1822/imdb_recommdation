from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-j-8.0.31.jar").master('local[*]').appName('rdstolocal').getOrCreate()                        
spark = SparkSession.builder.config("spark.jars", "s3://deprojectimdbbkt/mysql-connector-j-8.0.31.jar").master('local').appName('rdstos3').getOrCreate()   

df_title_basic = spark.read.format('jdbc').options(
      url='jdbc:mysql://i6629imdb.cdyarwjuez7j.us-east-1.rds.amazonaws.com:3306/rds_6629',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='title_basics', 
      user='admin',
      password='123456789').load()


df_title_akas = spark.read.format('jdbc').options(
      url='jdbc:mysql://i6629imdb.cdyarwjuez7j.us-east-1.rds.amazonaws.com:3306/rds_6629',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='title_akas', 
      user='admin',
      password='123456789').load()

df_title_ratings = spark.read.format('jdbc').options(
      url='jdbc:mysql://i6629imdb.cdyarwjuez7j.us-east-1.rds.amazonaws.com:3306/rds_6629', 
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='title_ratings',
      user='admin',
      password='123456789').load()

# df_title_ratings = spark.read.format("jdbc").option("url", "jdbc:mysql://i6629imdb.cdyarwjuez7j.us-east-1.rds.amazonaws.com:3306/6629_project") \
#     .option("dbtable", "title_ratings") \
#     .option("user", "admin").option("password", "123456789").load()

# df_title_ratings = spark.read.jdbc("jdbc:mysql://i6629imdb.cdyarwjuez7j.us-east-1.rds.amazonaws.com:3306","6629_project.title_ratings",properties={"user": "admin", "password": "123456789"})


# df_title_ratings.show(10)
# df_title_akas.show(10)
# df_title_basic.show(10)

# ### type casting columns ---------

# name_basics = name_basics.withColumn("birthYear",col("birthYear").cast(IntegerType())).withColumn("deathYear",col("deathYear").cast(IntegerType())) 
    
df_title_akas = df_title_akas.withColumn("isOriginalTitle",col("isOriginalTitle").cast(BooleanType())) 


df_title_basic = df_title_basic.withColumn("isAdult",col("isAdult").cast(BooleanType())) \
    .withColumn("startYear",col("startYear").cast(IntegerType())) \
    .withColumn("endYear",col("endYear").cast(IntegerType())) \
    .withColumn("runtimeMinutes",col("runtimeMinutes").cast(IntegerType()))
    
    
# ### data cleaning --------------

## title basic --------------

df_title_basic =  df_title_basic.na.fill(value=2200,subset=["endYear"])
df_title_basic.dropDuplicates()

temp_title_basic = df_title_basic.filter(df_title_basic.startYear.isNull())  #temp table for null values
df_title_basic = df_title_basic.filter(~(df_title_basic.startYear.isNull()))  
df_title_basic = df_title_basic.filter(~(df_title_basic.titleType.isNull()))
df_title_basic = df_title_basic.filter(df_title_basic.startYear < df_title_basic.endYear)
df_title_basic = df_title_basic.filter(~(df_title_basic.primaryTitle.isNull() | isnan(df_title_basic.primaryTitle) | df_title_basic.originalTitle.isNull() | isnan(df_title_basic.originalTitle)))
# #### Handle for titleType Nulls

# # ## ratings ----------------

df_title_ratings = df_title_ratings.filter((df_title_ratings.averageRating > 0) & (df_title_ratings.numVotes > 0))
df_title_ratings.dropDuplicates()


# ## title akas -------------

df_title_akas = df_title_akas.dropDuplicates()
df_title_akas = df_title_akas.replace(r'\N', None)

df_title_akas = df_title_akas.filter(~(df_title_akas.types.isNull()))
df_title_akas = df_title_akas.filter((~(df_title_akas.types == "original")))

df_title_akas = df_title_akas.drop(df_title_akas.isOriginalTitle)
df_title_akas = df_title_akas.drop(df_title_akas.attributes)
df_title_akas = df_title_akas.drop(df_title_akas.language) 

df_title_akas = df_title_akas.groupBy("titleId").agg(collect_list("types").alias('types'))

# ### df for rds

df_basic_rating = df_title_basic.join(df_title_ratings, df_title_basic.tconst == df_title_ratings.tconst,"inner"). \
    select(df_title_basic.tconst, df_title_basic.titleType, df_title_basic.primaryTitle, df_title_basic.originalTitle, \
        df_title_basic.startYear,df_title_basic.endYear, df_title_ratings.averageRating,df_title_ratings.numVotes)

df_basic_akas = df_title_basic.join(df_title_akas, df_title_akas.titleId == df_title_basic.tconst,"inner"). \
    select(df_title_basic.tconst, df_title_basic.titleType, df_title_basic.primaryTitle, df_title_basic.originalTitle, \
        df_title_basic.startYear,df_title_basic.endYear,df_title_akas.types)

df_basic_akas_rating = df_title_basic.join(df_title_akas,df_title_akas.titleId == df_title_basic.tconst,"inner"). \
                                      join(df_title_ratings,df_title_ratings.tconst == df_title_basic.tconst,"inner"). \
                                    select(df_title_basic.tconst, df_title_basic.titleType, df_title_basic.primaryTitle, df_title_basic.originalTitle, \
        df_title_basic.startYear,df_title_basic.endYear, df_title_ratings.averageRating,df_title_ratings.numVotes,df_title_akas.types)



# ####  Writing cleaned data to S3

df_title_basic.coalesce(1).write.mode('overwrite').option("header",True).partitionBy('titleType').parquet("s3://deprojectimdbbkt/cleanRdsData/title_basics")

df_title_ratings.coalesce(1).write.mode('overwrite').option("header",True).parquet("s3://deprojectimdbbkt/cleanRdsData/title_ratings")

df_title_akas.coalesce(1).write.mode('overwrite').option("header",True).parquet("s3://deprojectimdbbkt/cleanRdsData/title_akas")


df_basic_rating.coalesce(1).write.mode('overwrite').option("header",True).partitionBy('titleType').parquet("s3://deprojectimdbbkt/RDSedata/basic_rating")
df_basic_akas.coalesce(1).write.mode('overwrite').option("header",True).partitionBy('titleType').parquet("s3://deprojectimdbbkt/RDSedata/basic_akas")
df_basic_akas_rating.coalesce(1).write.mode('overwrite').option("header",True).partitionBy('titleType').parquet("s3://deprojectimdbbkt/RDSedata/basic_akas_rating")

# df_basic_rating.show(10)                   

