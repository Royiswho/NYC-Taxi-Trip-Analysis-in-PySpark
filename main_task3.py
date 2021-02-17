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
        if isstr(p[2]) and isfloat(p[5]) and isfloat(p[12]):
            if len(p[2]) == 19 and float(p[5]) > 0 and float(p[12]) > 0:
                return p

def mySequenceFunction(x, y):
    x.append(y)
    return x
def myCombinerFunction(x, y):
    x.extend(y)
    return x

def earnPerMile(s):
    earn = sum([float(i[1]) for i in s])
    mile = sum([float(i[0]) for i in s])

    return earn / mile

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: BestHour <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="BestHour")
    lines = sc.textFile(sys.argv[1], 1)
    taxilines = lines.map(lambda x: x.split(","))

    # cleaning up data
    texilinesCorrected = taxilines.filter(correctRows)

    # extract from certain fields
    rdd1 = texilinesCorrected.map(lambda x: (x[2][11:13], [x[5], x[12]]))
    # aggregate values based on key
    rdd2 = rdd1.aggregateByKey(list([]), mySequenceFunction, myCombinerFunction)
    # calculate earn per mile
    rdd3 = rdd2.map(lambda x: (x[0], earnPerMile(x[1])))
    # get top 1
    res = rdd3.top(1, lambda x: x[1])

    # now we want to store this result in a single file on the cluster.
    dataToASingleFile = sc.parallelize(res).coalesce(1)

    dataToASingleFile.saveAsTextFile(sys.argv[2])

    sc.stop()
