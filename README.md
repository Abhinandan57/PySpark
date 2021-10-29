# PySpark
PySpark is the Python API written in python to support Apache Spark.

# Spark (Not a Programming language, but a Framework)
Apache Spark is an open-source, parallel data processing framework that complements Apache Hadoop to make it easy to develop fast, unified Big Data Applications combining batch, streaming & interactive analytics.

# Spark Ecosystem
__1. Spark Core__
- It is the home to the API that represents RDD, which is the primary programming abstraction of Spark. It includes the primary functionality of Spark, namely components for task scheduling, fault recovery, memory management, interacting with storage systems etc.

__2. Spark SQL__
- It is the package for working with structured data. It enables querying data through SQL and as the Apache Hive variant of SQL (HQL).

__3. Spark Streaming__
- It is the component of Spark that allows live-streaming data processing.

__4. MLlib__
- It is the Spark's Machine Learning library.

__5. GraphX__
- It is a component in Spark for graphs and graph-parallel computation.

# Spark Architecture
![image](https://user-images.githubusercontent.com/26693264/139455917-b5150f0f-d105-4e52-98b2-6267aad454dc.png)

In Master node, you have the Driver Program, which drives the application. The code that you are writing behaves as a driver program or if you are using the interactive shell, the shell acts as the driver program.

Inside the driver program, the first thing that you do is, you create a SparkContext. SparkContext is the gateway to all spark functionality. It is similar to database connection. Any command you execute in your database, it goes through the database connection. Similarly anything you do on Spark goes through the SparkContext.

The Driver program and the SparkContext takes care of executing the job across the cluster. A job is splitted into tasks and then these tasks are distributed across the worker node. Worker nodes are the slave nodes whose job is basically to execute the tasks. The task is well executed on the partitioned RDDs in the worker nodes and then returns the result back to the SparkContext. If you increase the number of workers, then you can divide jobs in more partitions and execute them parallelly over multiple systems. This will actually be lot more faster. Also, if you increase the no. of workers, it will increase your memory and you can cache the jobs so that it can be executed much more faster. 

# RDD (Resilient Distributed Dataset)
It is the fundamental data structure of Spark. RDD is an immutable distributed collection of datasets. Dataset can be anything, like strings, lines, rows, objects, collections etc. Each dataset in RDD is divided into logical partitions which may be computed on different nodes of the cluster. Due to this you can perform transformations and actions on the complete data parallelly. Spark takes care of that distribution.

Once RDD is created, the content in it can't be changed. When transformations are performed, Spark runs those on old RDD and creates a new RDD. This is basically done for optimization reason.

# Spark SQL Libraries
1. Data Source API
2. DataFrame API
DataFrame API converts the data that is read through Data Source API into tabular columns to help perform SQL operations.
4. Interpreter & Optimizer
5. SQL Service

SparkSQL is designed to make it easier to work with structured data. SparkSession helps to operate SparkSQL.


# Spark vs. MapReduce
1. Spark is easy to program and doesn't require that much hand coding whereas MapReduce is not that easy in terms of programming and requires lots of hand coding.
2. Spark has interactive mode whereas in MapReduce ther is no built-in interactive mode. MapReduce is developed for batch processing.
3. For data processing Spark can use streaming, machine learning and batch processing; whereas Hadoop MapReduce uses the batch engine.
4. Spark executes batch processing jobs about 10 to 100 times faster than Hadoop MapReduce.
5. Spark uses lower latency by caching partial/complete results across distributed nodes whereas MapReduce is completely disk based.
6. Spark needs higher RAM whereas MapReduce needs larger disk space in terms of big data processing. On the cloud, Spark will definitely outperform MapReduce.
