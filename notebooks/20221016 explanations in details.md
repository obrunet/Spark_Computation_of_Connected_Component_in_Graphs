```python
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


BUCKET_INPUT_PATH = "gs://iasd-input-data"
BUCKET_OUTPUT_PATH = "gs://iasd-output"
NB_WORKER_NODES = 1  # ------- to be changed at each run / used in results ------- #


# create a spark session and retrieve the spark context from it
spark_session = SparkSession \
    .builder \
    .appName("PySpark App by Olivier & Jean-Loulou") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark_context = spark_session.sparkContext

# initialize nb_new_pair as a spark accumulator
nb_new_pair = sc.accumulator(0)


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


# countnb_new_pair function to know if additional CCF iteration is needed
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
    count new pairs, the return DFrrrrrrrrrrrrrrrrrrrrrrrr"""    
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
        "test_test": f"{BUCKET_INPUT_PATH}/test.txt"
#         "notre_dame": f"{BUCKET_INPUT_PATH}/web-NotreDame.txt",
#         "berk_stan": f"{BUCKET_INPUT_PATH}/web-BerkStan.txt",
#         "stanford": f"{BUCKET_INPUT_PATH}/web-Stanford.txt",
#         "google": f"{BUCKET_INPUT_PATH}/web-Google.txt"
    }
    computation_methods = {
        "rdd": workflow_rdd,
        "df": workflow_df
    }
    
    # loop on all the datasets & methods (RDD or DF) using previsous dictionnaries
    for dataset in dataset_paths.keys():
        for method in computation_methods.keys():
            print("\n"* 3 + "_" * 10 + 
                  f" nb of clusters' nodes: {NB_WORKER_NODES} - dataset: {dataset} - method: {method} "
                  + "_" * 10)
            computation_methods[method](dataset_paths[dataset])


if __name__ == "__main__":
    main()
```

    
    
    
    __________ nb of clusters' nodes: 1 - dataset: test_test - method: rdd __________


                                                                                    

    Number of new pairs for iteration #1:	8
    Number of new pairs for iteration #2:	30
    Number of new pairs for iteration #3:	47


                                                                                    

    Number of new pairs for iteration #4:	51


                                                                                    

    Number of new pairs for iteration #5:	51
    
    No new pair, end of computation


                                                                                    

    Nb of connected components in the graph: 2
    Duration in seconds: 26.928279638290405
    
    
    
    __________ nb of clusters' nodes: 1 - dataset: test_test - method: df __________


                                                                                    

    Number of new pairs for iteration #1:	55


                                                                                    

    Number of new pairs for iteration #2:	64
    Number of new pairs for iteration #3:	68


                                                                                    

    Number of new pairs for iteration #4:	68
    
    No new pair, end of computation
    Nb of connected components in the graph: 2
    Duration in seconds: 18.64290738105774



```python
    rdd_raw = load_rdd(path)
    rdd = preprocess_rdd(rdd_raw)
    start_time = time.time()
    rdd = compute_cc_rdd(rdd)
```


```python
    df_raw = load_df(path)
    df = preprocess_df(df_raw)
    start_time = time.time()
    df = compute_cc_df(df)
```


```python
        df = iterate_map_df(df)
        df = iterate_reduce_df(df)
        df = df.distinct()
```


```python
path = f"{BUCKET_INPUT_PATH}/test.txt"
```

file loading


```python
rdd_raw = load_rdd(path)
rdd_raw.take(6)
```




    ['# bla bla', '# header', '1\t2', '2\t3', '2\t4', '4\t5']




```python
df_raw = load_df(path)
df_raw.show(6)
```

    +---------+
    |      _c0|
    +---------+
    |# bla bla|
    | # header|
    |      1	2|
    |      2	3|
    |      2	4|
    |      4	5|
    +---------+
    only showing top 6 rows
    


dataset preprocessing


```python
rdd = preprocess_rdd(rdd_raw)
rdd.take(10)
```




    [(1, 2), (2, 3), (2, 4), (4, 5), (6, 7), (7, 8)]




```python
df = preprocess_df(df_raw)
df.show(10)
```

    +---+---+
    |  k|  v|
    +---+---+
    |  1|  2|
    |  2|  3|
    |  2|  4|
    |  4|  5|
    |  6|  7|
    |  7|  8|
    +---+---+
    


iterate map


```python
rdd = iterate_map_rdd(rdd)
rdd.take(20)
```




    [(1, 2),
     (2, 3),
     (2, 4),
     (4, 5),
     (6, 7),
     (7, 8),
     (2, 1),
     (3, 2),
     (4, 2),
     (5, 4),
     (7, 6),
     (8, 7)]




```python
df = iterate_map_df(df)
df.show(20)
```

    +---+---+
    |  k|  v|
    +---+---+
    |  1|  2|
    |  2|  3|
    |  2|  4|
    |  4|  5|
    |  6|  7|
    |  7|  8|
    |  2|  1|
    |  3|  2|
    |  4|  2|
    |  5|  4|
    |  7|  6|
    |  8|  7|
    +---+---+
    


iterate reduce


```python
rdd = iterate_reduce_rdd(rdd)
rdd.take(16)
```




    [(2, 1),
     (3, 1),
     (3, 2),
     (4, 2),
     (4, 1),
     (5, 2),
     (5, 4),
     (7, 6),
     (8, 7),
     (8, 6)]




```python
df = iterate_reduce_df(df)
df.show()
```

    +---+---+
    |  k|  v|
    +---+---+
    |  2|  3|
    |  4|  5|
    |  2|  4|
    |  2|  5|
    |  6|  7|
    |  6|  8|
    |  7|  8|
    |  1|  2|
    |  1|  3|
    |  1|  4|
    +---+---+
    



```python

```
