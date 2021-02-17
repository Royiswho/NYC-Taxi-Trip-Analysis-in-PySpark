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


def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


# For example , remove lines if they don’t have 17 values and . . .
def correctRows(p):
    if (len(p) == 17):
        if isstr(p[1]) and isfloat(p[4]) and isfloat(p[16]):
            if p[1] != "" and float(p[4]) > 0 and float(p[16]) > 0:
                return p

def mySequenceFunction(x, y):
    x.append(y)
    return x

def myCombinerFunction(x, y):
    x.extend(y)
    return x

def earnPerMin(s):
    earn = sum([float(i[1]) for i in s])
    minute = sum([float(i[0]) for i in s]) / 60

    return earn / minute

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Top10BestDrivers <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top10BestDrivers")
    lines = sc.textFile(sys.argv[1], 1)
    taxilines = lines.map(lambda x: x.split(","))

    # cleaning up data
    texilinesCorrected = taxilines.filter(correctRows)

    # extract from certain fields
    rdd1 = texilinesCorrected.map(lambda x: (x[1], [x[4], x[16]]))
    # aggregate values based on key
    rdd2 = rdd1.aggregateByKey(list([]), mySequenceFunction, myCombinerFunction)
    # calculate earn per minute
    rdd3 = rdd2.map(lambda x: (x[0], earnPerMin(x[1])))
    # get top 10
    res = rdd3.top(10, lambda x: x[1])

    # now we want to store this result in a single file on the cluster.
    dataToASingleFile = sc.parallelize(res).coalesce(1)

    dataToASingleFile.saveAsTextFile(sys.argv[2])

    sc.stop()
