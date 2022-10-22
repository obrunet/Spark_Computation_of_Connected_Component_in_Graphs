#! /usr/bin/python

import pyspark

#Create List
numbers = [1,2,1,2,3,4,4,6]

#SparkContext
sc = pyspark.SparkContext()

# Creating RDD using parallelize method of SparkContext
rdd = sc.parallelize(numbers)

#Returning distinct elements from RDD
distinct_numbers = rdd.distinct().collect()

#Print
print('Distinct Numbers:', distinct_numbers)

# credits: https://www.freecodecamp.org/news/what-is-google-dataproc/