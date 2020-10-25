# -*- coding: utf-8 -*-
"""term_project.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1kSZN3xNufb85HUqK8hkMgRCsAJWLlj2x
"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
!tar xf spark-2.4.4-bin-hadoop2.7.tgz
!pip install -q findspark

# Commented out IPython magic to ensure Python compatibility.
from google.colab import drive
drive.mount('/content/drive',force_remount=True )
# %cd '/content/drive/My Drive/Colab Notebooks'

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark import SparkContext as sc
import pandas as pd
import os
import numpy as np
import json

from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, udf

from pyspark.sql import SparkSession
from pyspark import SparkFiles
import matplotlib.pyplot as plt

spark = SparkSession.builder.master("local[*]").getOrCreate()

business_df = spark.read.csv("business.csv", header=True)

from pyspark.sql.types import DoubleType
data_df = business_df.withColumn("stars", business_df["stars"].cast(DoubleType()))
data_df = data_df.withColumn("review_count", business_df["review_count"].cast(DoubleType()))
data_df = data_df.withColumn("weighted_ratio", data_df["weighted_ratio"].cast(DoubleType())).drop("is_open")
data_df = data_df.withColumn("category_count", data_df["category_count"].cast(DoubleType()))
data_df.show()

data_city = data_df.select("city","review_count","stars","weighted_ratio","categories","category_count").collect()
city =  [item[0] for item in data_city]
review_count = [item[1] for item in data_city]
stars =  [item[2] for item in data_city]
weighted_ratio = [item[3] for item in data_city]
categories =[item[4] for item in data_city]
category_count=[item[5] for item in data_city]
data_city = {"city" : city, "review_count": review_count, "stars":stars, "weighted_ratio":weighted_ratio,"categories":categories,"category_count":category_count}
data_city=pd.DataFrame(data_city)
city_business_reviews = data_city[['city', 'review_count', 'stars']].groupby(['city']).agg({'review_count':'sum','stars': 'mean'}).sort_values(by='review_count', ascending=False)
city_business_reviews['review_count'][0:20].plot(kind='bar', stacked=False, figsize=[10,10],colormap='winter')
plt.title('Top 20 cities by reviews')

city_weighted_ratio =data_city[['city', 'weighted_ratio']].groupby(['city']).agg({'weighted_ratio': 'sum'}).sort_values(by='weighted_ratio', ascending=False)

city_weighted_ratio['weighted_ratio'][0:20].plot(kind='bar', stacked=False, figsize=[10,10],colormap='summer')
plt.title('Top 20 cities by Weighted Rating')

import seaborn as sns
sns.regplot(x=data_city["stars"], y=data_city["category_count"], fit_reg=False)

categories_data=spark.read.csv("categories.csv", header=True)
categories =categories_data.withColumn("stars", categories_data["stars"].cast(DoubleType()))
categories =categories.withColumn("review_count", categories_data["review_count"].cast(DoubleType()))

category=categories.select("city","categories","state","stars","review_count").collect()

city =  [item[0] for item in category]
categories = [item[1] for item in category]
state =  [item[2] for item in category]
stars = [item[3] for item in category]
review_count=[item[4] for item in category]
category = {"city" : city, "categories": categories, "state":state, "stars":stars,"review_count":review_count}

df=pd.DataFrame(category)
df.info()

yelp_restaurants=data_city[['city','review_count','stars','weighted_ratio','categories','category_count']]

## select out 16 cuisine types of restaurants and rename the category
yelp_restaurants.is_copy=False
yelp_restaurants['category']=pd.Series()
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('American'),'category'] = 'American'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Mexican'), 'category'] = 'Mexican'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Italian'), 'category'] = 'Italian'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Japanese'), 'category'] = 'Japanese'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Chinese'), 'category'] = 'Chinese'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Thai'), 'category'] = 'Thai'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Mediterranean'), 'category'] = 'Mediterranean'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('French'), 'category'] = 'French'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Vietnamese'), 'category'] = 'Vietnamese'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Greek'),'category'] = 'Greek'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Indian'),'category'] = 'Indian'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Korean'),'category'] = 'Korean'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Hawaiian'),'category'] = 'Hawaiian'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('African'),'category'] = 'African'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Spanish'),'category'] = 'Spanish'
yelp_restaurants.loc[yelp_restaurants.categories.str.contains('Middle_eastern'),'category'] = 'Middle_eastern'
yelp_restaurants.category[:20]
yelp_restaurants=yelp_restaurants.dropna(axis=0, subset=['category'])
del yelp_restaurants['categories']
yelp_restaurants=yelp_restaurants.reset_index(drop=True)
yelp_restaurants.head(10)
yelp_restaurants.isnull().sum()

plt.figure(figsize=(11,7))
grouped = yelp_restaurants.groupby('category')['review_count'].sum().sort_values(ascending = False)
sns.barplot(y=grouped.index, x= grouped.values, palette= sns.color_palette("Set2", len(grouped)) )
plt.ylabel('Category', fontsize=14)
plt.xlabel('Count of reviews', fontsize=14)
plt.title("Popular Cuisine Type by Count of Reviews", fontsize=15)
for i,v in enumerate(grouped):
    plt.text(v, i+0.15, str(v),fontweight='bold', fontsize=14)
plt.tick_params(labelsize=14)

plt.figure(figsize=(11,7))
grouped = yelp_restaurants.groupby('category')['stars'].mean().sort_values(ascending = False)
sns.barplot(y=grouped.index, x= grouped.values, palette= sns.color_palette("husl", len(grouped)) )
plt.ylabel('Category', fontsize=14)
plt.xlabel('Average Customer Review', fontsize=14)
plt.title("Top Cuisine", fontsize=15)
for i,v in enumerate(grouped):
    plt.text(v, i+0.15, str(v),fontweight='bold', fontsize=14)
plt.tick_params(labelsize=14)

# # import seaborn as sns
# # sns.regplot(x=yelp_restaurants["stars"], y=yelp_restaurants["category_count"], fit_reg=False)
# # df1= df[['city','state','categories','stars','review_count']].groupby(['city','state','categories']).agg({'stars':'mean'})
# # df1.head
# grouped = yelp_restaurants.groupby('categories')['review_count'].sum().sort_values(ascending = False)
# sns.barplot(y=grouped.index, x= grouped.values, palette= sns.color_palette("RdBu_r", len(grouped)) )
# plt.ylabel('categories', fontsize=14)
# plt.xlabel('Count of reviews', fontsize=14)
# plt.title('Count of Reviews by Cuisine Type', fontsize=15)
# for i,v in enumerate(grouped):
#     plt.text(v, i+0.15, str(v),fontweight='bold', fontsize=14)
# plt.tick_params(labelsize=14)

plt.figure(figsize=(11,6))
grouped = yelp_restaurants.groupby('city')['review_count'].sum().sort_values(ascending=False)[:10]
sns.barplot(grouped.index, grouped.values, palette=sns.color_palette("Blues", len(grouped)) )
plt.xlabel('City', labelpad=10, fontsize=14)
plt.ylabel('Count', fontsize=14)
plt.title('Count of Reviews by City (Top 10)', fontsize=15)
plt.tick_params(labelsize=14)
plt.xticks(rotation=15)
for  i, v in enumerate(grouped):
    plt.text(i, v*1.02, str(v), horizontalalignment ='center',fontweight='bold', fontsize=14)

plt.figure(figsize=(11,6))
grouped = yelp_restaurants.stars.value_counts().sort_index()
sns.barplot(grouped.index, grouped.values, palette=sns.color_palette("cubehelix", len(grouped)))
plt.xlabel('Average Rating', labelpad=10, fontsize=14)
plt.ylabel('Count of restaurants', fontsize=14)
plt.title('Count of Restaurants against Ratings', fontsize=15)
plt.tick_params(labelsize=14)
for  i, v in enumerate(grouped):
    plt.text(i, v*1.02, str(v), horizontalalignment ='center',fontweight='bold', fontsize=14)

plt.figure(figsize=(11,7))
yelp_restaurants.city.unique
grouped = yelp_restaurants[yelp_restaurants["city"]=="Bridgeville"].groupby('category')['review_count'].mean().sort_values(ascending = False)
print(grouped.head(10))
sns.barplot(y=grouped.index, x= grouped.values, palette= sns.color_palette("GnBu_d", len(grouped)) )
plt.ylabel('Category', fontsize=14)
plt.xlabel('Count of reviews', fontsize=14)
plt.title('Count of Reviews by Cuisine Type', fontsize=15)
for i,v in enumerate(grouped):
    plt.text(v, i+0.15, str(v),fontweight='bold', fontsize=14)
plt.tick_params(labelsize=14)
