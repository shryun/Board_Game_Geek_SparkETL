# Databricks notebook source
# MAGIC %md ###Board Game Geek - Spark ETL Project
# MAGIC Board Game Geek is an online site that provides rankings for various boardgames.  A complete listing of ranked games is provided here: https://boardgamegeek.com/browse/boardgame
# MAGIC
# MAGIC The project is to scrape board game ranking data from BoardGameGeek and store it as a partitioned Parquet table for analytics.
# MAGIC

# COMMAND ----------

# MAGIC %md The fields in the final table are:
# MAGIC
# MAGIC * Rank - the integer value from the Board Game Rank column
# MAGIC * ImageURL - the URL (as provided) of the image presented in the second column 
# MAGIC * Title - the title of the game
# MAGIC * YearPublished - the year the game was published
# MAGIC * GeekRating - the float value presented in the Geek Rating field
# MAGIC * AvgRating - the float value presented in the Avg Rating field
# MAGIC * NumVoters - the integer value presented in the NumVoters field

# COMMAND ----------

# run this code to make sure beautiful soup is installed
# dbutils.library.installPyPI('beautifulsoup4')
%pip install beautifulsoup4
dbutils.library.restartPython()

# COMMAND ----------

# retrieve the web page

from urllib import request as url

request = url.Request('https://boardgamegeek.com/browse/boardgame')
response = url.urlopen(request)

html = response.read()

print(html)

# COMMAND ----------

from bs4 import BeautifulSoup

soup = BeautifulSoup(html)
print(soup)

# COMMAND ----------

# extract the data from the web page   

import pandas as pd

soup = BeautifulSoup(html, 'html.parser')

all_data = []

# get all tables in the page
tables = soup.find_all('table')

# Extract header
header = []
for th in tables[0].find_all('th'):
  header.append(th.get_text(strip=True))

# get data from each table
for table in tables:
  table_data = []
  for tr in table.find_all('tr'):
      row_data = []
      for td in tr.find_all('td'):
          img_tag = td.find('img')
          if img_tag:
              row_data.append(img_tag['src'])
          else:
              row_data.append(td.get_text(strip=True))
      table_data.append(row_data)
  all_data.append(table_data)
  
print(header)
print(all_data)

# COMMAND ----------

# convert the data to a Spark SQL table
df = pd.DataFrame(all_data[0], columns=header)

BGG = spark.createDataFrame(df)

display(BGG)

# COMMAND ----------

# cleanup
import pyspark.sql.functions as f
from datetime import datetime
import re

# extract year
def extract_year_from_title(title):
    year_match = re.search(r'\((\d{4})\)', title)
    if year_match:
        return int(year_match.group(1))
    else:
        return None
      
extract_year_udf = f.udf(extract_year_from_title)

# clean up data in the dataframe
clean_BGG = (
  BGG
    .distinct() # remove duplicate rows
    .withColumnRenamed("Board Game Rank", "Rank")
    .withColumnRenamed("Thumbnail image", "ImageURL")
    .withColumnRenamed("Geek Rating", "GeekRating")
    .withColumnRenamed("Avg Rating", "AvgRating")
    .withColumnRenamed("Num Voters", "NumVoters")
    .drop("shop")
    .dropna(how='any')
    .withColumn('YearPublished', extract_year_udf(f.col('Title')))
    .withColumn('currentdate', f.lit(datetime.now().strftime("%Y-%m-%d")))
)

clean_BGG = clean_BGG.withColumn('Title', f.substring_index(clean_BGG['Title'], '(', 1))

clean_BGG = clean_BGG.select('Rank', 'ImageURL', 'Title', 'YearPublished', 'GeekRating', 'AvgRating', 'NumVoters', 'currentdate')
    
display(clean_BGG)

# COMMAND ----------

# convert the data to a Spark SQL table
spark.sql('CREATE DATABASE IF NOT EXISTS games')
 
# save ufo sightings data as a table
(
  clean_BGG
    .write
    .partitionBy('currentdate')
    .format('parquet')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .option('path','/tmp/games')
    .saveAsTable('games.board_games')
  )
 
# read data from table
display(
  spark.table('games.board_games')
  )

# COMMAND ----------

# MAGIC %md Execute the following SQL statements to validate your results:

# COMMAND ----------

# MAGIC %sql -- verify row count
# MAGIC
# MAGIC SELECT COUNT(*) FROM games.board_games;

# COMMAND ----------

# MAGIC %sql -- verify values
# MAGIC
# MAGIC SELECT * FROM games.board_games ORDER BY rank ASC;

# COMMAND ----------

# MAGIC %sql -- verify partitioning
# MAGIC DESCRIBE EXTENDED games.board_games;