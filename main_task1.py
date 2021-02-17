from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


# Exception Handling and removing wrong datalines
def isstr(value):
    try:
        str(value)
        return True
    except:
        return False


# For example , remove lines if they don’t have 17 values and . . .
def correctRows(p):
    if (len(p) == 17):
        if isstr(p[0]) and isstr(p[1]):
            if p[0] != "" and p[1] != "":
                return p

def mySequenceFunction(x, y):
    x.add(y)
    return x

def myCombinerFunction(x, y):
    x.update(y)
    return x

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Top10ActiveTaxi <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="Top10ActiveTaxi")
    lines = sc.textFile(sys.argv[1], 1)
    taxilines = lines.map(lambda x: x.split(","))

    # cleaning up data
    texilinesCorrected = taxilines.filter(correctRows)

    # extract from certain fields
    rdd1 = texilinesCorrected.map(lambda x: (x[0], x[1]))
    # aggregate unique values based on key
    rdd2 = rdd1.aggregateByKey(set([]), mySequenceFunction, myCombinerFunction)
    # count the number of drivers
    rdd3 = rdd2.map(lambda x: (x[0], len(x[1])))
    # get top 10
    res = rdd3.top(10, lambda x: x[1])

    # now we want to store this result in a single file on the cluster.
    dataToASingleFile = sc.parallelize(res).coalesce(1)

    dataToASingleFile.saveAsTextFile(sys.argv[2])
    
    sc.stop()
