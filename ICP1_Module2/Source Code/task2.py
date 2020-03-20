import os

os.environ["SPARK_HOME"] = "file:///usr/local/cellar/apache-spark/2.4.5"
from operator import add
def hashp(x):
    return hash(x)

from pyspark import SparkContext
from pyspark import *

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("file:///Users/dineshkumar/Downloads/ICP1/sampletext2.txt", 1)

    tokens = lines.flatMap(lambda x:x.split('\n'))
    print(tokens.collect())
    r=tokens.collect()[0].split(',')[0]
    li=tokens.map(lambda x:x.split(','))

    pairs=li.map(lambda x:(x[0]+'-'+x[1],x[3]))
    pairs=pairs.partitionBy(4,hashp)
    print(pairs.getNumPartitions())
    pairs1=pairs.groupByKey()
    print(pairs1)

    q=[]
    for x,y in pairs1.collect():
        for m in y:
            q.append(int(m))
            j=sorted(q,reverse=True)

        with open("/Users/dineshkumar/Downloads/ICP1/output2","a+") as f:
            f.write(str(x)+","+str(j)+"\n")
        print(str(x),j)


