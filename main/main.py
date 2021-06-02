from pyspark.sql import SparkSession
import sys
import utils
import hard
# import cords, hard, delta, soft
# from enum import Enums



# connect to spark and read data
# spark = SparkSession.builder.getOrCreate()
# df = spark.read.csv('data.csv', header=True, inferSchema=True, mode='PERMISSIVE', encoding='ISO-8859-1')



# get some friendly help
if len(sys.argv) == 2 and sys.argv[1] == '-h':
    print('USAGE:')
    print('-h\t\t', 'You must already now this one ;)')
    print('-r [int]\t', 'Limit the number of dataset rows')
    print('-c [int]\t', 'Limit the numer of database columns')
    print('-e [int]\t', 'Specifiy a desired error bound between 0 and 1 (default)')
    print('-a [int]\t', 'Specifiy the algorithm to run')
    print('\t\t', '0: CORDS pre-filter')
    print('\t\t', '1: (hard) functional dependencies')
    print('\t\t', '2: delta dependencies')
    print('\t\t', '3: soft dependencies')
    print()
    print('OTHER:')
    print('Use \'deltas.txt\' to specifiy per-column delta limits')
    exit(0)



# parse command line arguments
rowLimit = -1
colLimit = -1
errorBound = 1
alg = 1
for i in range(1, len(sys.argv), 2):
    opt = sys.argv[i]
    arg = sys.argv[i+1]
    # limit rows
    if opt == '-r':
        rowLimit = int(arg)
        df = df.limit(rowLimit)
    # limit cols
    if opt == '-c':
        colLimit = int(arg)
    if opt == '-e':
        errorBound = int(arg)
    if opt == '-a':
        alg = int(opt)



# load user-specified delta limits
deltas = dict()
with open('deltas.txt') as f:
    for line in f.readlines():
        columnName, delta = line.split(' ')
        deltas[columnName] = int(delta)



# load found dependencies
cords_deps = utils.read_dependencies('./found_deps/cords.json')
found_FDs = utils.read_dependencies('./found_deps/fds.json')
found_DDs = utils.read_dependencies('./found_deps/dds.json')
found_SDs = utils.read_dependencies('./found_deps/sds.json')



# run specified algorithm
if alg == 0:
    # CORDS pre-filter
    print('TODO')
elif alg == 1:
    # (hard) functoinal dependencies
    found_FDs = hard.findFDs(df, lhsSizes=[1,2,3], sampleRates=[0.1], colLimit=colLimit)
    utils.write_dependencies('./found_deps/fds.json')
elif alg == 2:
    # delta dependencies
    print('TODO')
elif alg == 3:
    # soft functional dependencies
    print('TODO')
else:
    print('Not a valid algorithm specifier')
