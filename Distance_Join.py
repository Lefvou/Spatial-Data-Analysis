import math
import pandas as pd
from datetime import datetime
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode_outer
from dataset import Dataset

findspark.init()
spark = SparkSession.builder.master("yarn").getOrCreate()
sqlContext = SQLContext(spark)

# SET NODES,DISTANCE,PATHS, Export Status
nodes = 128
distk = 0.1
file1 = "hdfs:////user/hadoopuser/df1.csv"
file2 = "hdfs:////user/hadoopuser/df2.csv"
export = False

start_stamp = datetime.now()
# Read Datasets
datasetA = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ",").option("header", "True").load(file1)
datasetB = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ",").option("header", "True").load(file2)

# KD-TREE ALGORITHM
gdatasetA = datasetA.select("x", "y")
max_level = int(math.log(nodes,2))
df_instance = Dataset(gdatasetA.toPandas())
del gdatasetA
df_instance.do_work(max_level)
str_stamp2 = datetime.now()

# REGISTER DATAFRAMES TO SQL CONTEXT
datasetA.createOrReplaceTempView("datasetA")
datasetB.createOrReplaceTempView("datasetB")

# CREATE MBRs
datasetA_boxs = spark.sql("SELECT *, 0 AS Box FROM datasetA WHERE 1=0")
datasetB_boxs = spark.sql("SELECT *, 0 AS Box FROM datasetB WHERE 1=0")
Box=0

for box in df_instance.median_boxes:
    y_min = box.y_1
    y_max = box.y_2
    x_min = box.x_1
    x_max = box.x_2
    
    datasetA_box = spark.sql(
        "SELECT *, {0} AS Box FROM datasetA WHERE x >= {1} AND x <= {2} AND y >= {3} AND y <= {4}".format(
            Box, x_min, x_max, y_min, y_max))
    datasetA_boxs = datasetA_boxs.union(datasetA_box)
    datasetB_box = spark.sql(
        "SELECT *, {0} AS Box FROM datasetB WHERE x >= {1} AND x <= {2} AND y >= {3} AND y <= {4}".format(
            Box, x_min-distk, x_max+distk, y_min-distk, y_max+distk))
    datasetB_boxs = datasetB_boxs.union(datasetB_box)
    Box +=1

# REPARTITION DATAFRAMES BASED ON BOXES AND KD-TREE NODES##
from pyspark.sql.functions import col 

datasetA_boxs = datasetA_boxs.repartition(nodes, col("Box"))
datasetB_boxs = datasetB_boxs.repartition(nodes, col("Box"))

# REGISTER DATAFRAMES TO SQL CONTEXT #
datasetA_boxs.createOrReplaceTempView("datasetA_boxs")
datasetB_boxs.createOrReplaceTempView("datasetB_boxs")

# DISTANCE JOIN STATEMENT
Distance_joins = spark.sql("SELECT a.IDA,b.IDB, (POWER(POWER(b.x - a.x,2) + POWER(b.y - a.y,2),0.5))  as distance FROM datasetA_boxs a INNER JOIN datasetB_boxs b WHERE a.Box = b.Box AND (POWER(b.x - a.x,2) +  POWER(b.y - a.y,2)) <{0}".format(distk * distk))

# DISTANCE JOIN EXECUTE
print("Total Joins: ",Distance_joins.count())
end_stamp = datetime.now()

# EXPORT RESULTS TO CSV
if export == True:
    Distance_joins.write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("Results.csv")

algorithm = (end_stamp - start_stamp).total_seconds()
print("Execution Time  of algorithm: {0}s".format(algorithm))
