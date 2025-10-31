# Databricks notebook source
spark


# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/skilllab/marva/netflix_titles_CLEANED.csv", header=True, inferSchema=True)


# COMMAND ----------

display(df)       # Databricks Display
df.show(10)       # Show first 10 rows
df.show(truncate=False)   # Disable text cutoff


# COMMAND ----------

df.printSchema()


# COMMAND ----------

df.dtypes


# COMMAND ----------

df = df.dropDuplicates()


# COMMAND ----------

df = df.fillna("Not Available")


# COMMAND ----------

df2 = df.select("show_id", "title", "countries", "release_year")
display(df2)


# COMMAND ----------

movies_df = df.filter(df.type == "Movie")
display(movies_df)


# COMMAND ----------

df_sorted = df.orderBy(df.release_year.desc())
display(df_sorted)


# COMMAND ----------

from pyspark.sql.functions import count

type_count = df.groupBy("type").agg(count("*").alias("total_titles"))
display(type_count)


# COMMAND ----------

from pyspark.sql.functions import upper

df = df.withColumn("title_upper", upper(df.title))


# COMMAND ----------

df.createOrReplaceTempView("netflix")


# COMMAND ----------

df.write.mode("overwrite").parquet("/Volumes/workspace/skilllab/marva/netflix_titles_CLEANED.csv")


# COMMAND ----------

