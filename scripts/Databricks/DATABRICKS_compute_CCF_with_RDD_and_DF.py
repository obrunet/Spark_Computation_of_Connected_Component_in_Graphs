"""
# Title : PySpark Script to compute connected components of graph
# Description : 
# Author : O. Brunet & J.L. Lezaun
# Date : Oct. 22
# Version : final
"""

import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


# create a spark session and retrieve the spark context from it
spark_session = SparkSession \
    .builder \
    .appName("PySpark App by Olivier & Jean-Loulou") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark_context = spark_session.sparkContext

# initialize nb_new_pair as a spark accumulator
nb_new_pair = spark_context.accumulator(0)


def load_rdd(path):
    """Recieve the path of file to load and return an RDD of the dataset"""
    return spark_context.textFile(path)


def load_df(path):
    """Recieve the path of file to load and return a DF of the dataset"""
    return spark_session.read.format("csv").option("header","false")\
                .load(path)


def preprocess_rdd(rdd):
    """Recieve an RDD with the raw data and return a new RDD without multilines headers
    starting with '#', and columns splitted according to the tab separator"""
    return rdd.filter(lambda x: "#" not in x) \
                .map(lambda x: x.split("\t")) \
                .map(lambda x: (int(x[0]), int(x[1])))


def preprocess_df(df):
    """Recieve a DF with the raw data and return a new DF without multilines headers
    starting with '#', and columns splitted according to the tab separator"""
    col_name = df.columns[0]
    return df.filter(f"{col_name} NOT LIKE '#%'")\
                .withColumn('k', split(df[col_name], '\t').getItem(0)) \
                .withColumn('v', split(df[col_name], '\t').getItem(1)) \
                .drop(col_name)\
                .withColumn("k",col("k").cast(IntegerType())) \
                .withColumn("v",col("v").cast(IntegerType()))


def iterate_map_rdd(rdd):
    """Recieve an RDD with (k, v) and return a new RDD resulting of the concatenation of the 
    original input RDD and itself where keys & values were inverted (v, k)"""
    return rdd.union(rdd.map(lambda x : (x[1], x[0])))


def iterate_map_df(df):
    """Recieve a DF with 2 columns 'k', 'v' and return a new DF resulting of the concatenation 
    of the original input DF and itself where columns were inverted 'k', 'v'"""
    return df.union(df.select(col("v").alias("k"), col("k").alias("v")))


def count_nb_new_pair(x):
    """Count the number of new pairs - function used in iterate_reduce_rdd in order to 
    determine if new edges were attached in a component and if the process is over or not"""
    global nb_new_pair
    k, values = x
    min, value_list = k, []
    for v in values:
        if v < min:
            min = v
        value_list.append(v)
    if min < k:
        yield((k, min))
        for v in value_list:
            if min != v:
                nb_new_pair += 1
                yield((v, min))
        

def iterate_reduce_rdd(rdd):
    """Recieve an RDD alreday processed by iterate_map_rdd(), and for each component
    count new pairs with count_nb_new_pair(), the return RDD is sorted by keys"""
    return rdd.groupByKey().flatMap(lambda x: count_nb_new_pair(x)).sortByKey()


def iterate_reduce_df(df):
    """Recieve a DF alreday processed by iterate_map_df(), and for each component
    count new pairs, the return DF transformed"""    
    global nb_new_pair
    df = df.groupBy(col("k")).agg(collect_set("v").alias("v"))\
                                            .withColumn("min", least(col("k"), array_min("v")))\
                                            .filter((col("k")!=col('min')))

    nb_new_pair += df.withColumn("count", size("v")-1).select(sum("count")).collect()[0][0]

    return df.select(col("min").alias("a_min"), concat(array(col("k")), col("v")).alias("valueList"))\
                                                    .withColumn("valueList", explode("valueList"))\
                                                    .filter((col('a_min')!=col('valueList')))\
                                                    .select(col('a_min').alias("k"), col('valueList').alias("v"))


def compute_cc_rdd(rdd):
    """Recieve a preprocessed RDD and compute CCF according to several iterations jobs of iterate_map/reduce_rdd, dedup.
    When no new pair were counted: return an RDD with the componentIDs of each group of connected edges"""
    nb_iteration = 0
    while True:
        nb_iteration += 1
        start_pair = nb_new_pair.value

        rdd = iterate_map_rdd(rdd)
        rdd = iterate_reduce_rdd(rdd)
        rdd = rdd.distinct()  # used for iterate_dedup / deduplication

        print(f"Number of new pairs for iteration #{nb_iteration}:\t{nb_new_pair.value}")
        if start_pair == nb_new_pair.value:
            print("\nNo new pair, end of computation")
            break
    return rdd


def compute_cc_df(df):
    """Recieve a preprocessed DF and compute CCF according to several iterations jobs of iterate_map/reduce_df, dedup.
    When no new pair were counted: return a DF with the componentIDs of each group of connected edges"""
    nb_iteration = 0
    while True:
        nb_iteration += 1
        nb_pairs_start = nb_new_pair.value

        df = iterate_map_df(df)
        df = iterate_reduce_df(df)
        df = df.distinct()
        
        print(f"Number of new pairs for iteration #{nb_iteration}:\t{nb_new_pair.value}")
        if nb_pairs_start == nb_new_pair.value:
            print("\nNo new pair, end of computation")
            break
    return df


def workflow_rdd(path):
    """Recieve the dataset path, and realize all the transformations / computation in RDD: loading,
    preprocessing, CCF computation, calculate time and the number of distinct connected components"""
    rdd_raw = load_rdd(path)
    rdd = preprocess_rdd(rdd_raw)
    start_time = time.time()
    rdd = compute_cc_rdd(rdd)
    print(f"Nb of connected components in the graph: {rdd.map(lambda x : x[1]).distinct().count()}")
    print(f"Duration in seconds: {time.time() - start_time}")


def workflow_df(path):
    """Recieve the dataset path, and realize all the transformations / computation in DF: loading,
    preprocessing, CCF computation, calculate time and the number of distinct connected components"""
    df_raw = load_df(path)
    df = preprocess_df(df_raw)
    start_time = time.time()
    df = compute_cc_df(df)
    print(f"Nb of connected components in the graph: {df.select('k').distinct().count()}")
    print(f"Duration in seconds: {time.time() - start_time}")   
    

def main():
    
    dataset_paths = {
         "notre_dame": "/FileStore/tables/web_NotreDame.txt",
         "berk_stan": "/FileStore/tables/web_BerkStan.txt",
         "stanford": "/FileStore/tables/web_Stanford.txt",
         "google": "/FileStore/tables/web_Google-2.txt"
    }
    
    computation_methods = {
        "rdd": workflow_rdd,
        "df": workflow_df
    }
    
    # loop on all the datasets & methods (RDD or DF) using previsous dictionnaries
    for dataset in dataset_paths.keys():
        for method in computation_methods.keys():
            print("\n"* 3 + "_" * 10 + 
                  f" dataset: {dataset} - method: {method} "
                  + "_" * 10)
            computation_methods[method](dataset_paths[dataset])


if __name__ == "__main__":
    main()