import os

os.environ["SPARK_HOME"] = "file:///usr/local/cellar/apache-spark/2.4.5"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("file:///Users/dineshkumar/Downloads/ICP1/sampletext1.txt", 1)

    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    counts.saveAsTextFile("file:///Users/dineshkumar/Downloads/ICP1/output")
    sc.stop()

