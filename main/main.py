from pyspark.sql import SparkSession
import sys
import utils
import hard, soft, cords
from datetime import datetime
import argparse



# connect to spark and read data
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('preprocessed_data.csv', header=True, inferSchema=True, mode='PERMISSIVE', encoding='ISO-8859-1')



# argparse rewrite
parser = argparse.ArgumentParser()
parser.add_argument('--rows', '-r', type=int, nargs='?', help='Limit the number of database rows')
parser.add_argument('--cols', '-c', type=int, nargs='?', help='Limit the number of database columns')
parser.add_argument('--tau', '-t', type=float, nargs='?', help='Set the soft dependency threshold tau')
parser.add_argument('--alg', '-a', help='Run the given algorithm')
parser.add_argument('--cords', type=bool, help='Use the CORDS pre-filter () or not ()')


# get some friendly help
if len(sys.argv) == 2 and sys.argv[1] == '-h':
    print('USAGE:')
    print('-h\t\t', 'You must already now this one ;)')
    print('-r [int]\t', 'Limit the number of dataset rows')
    print('-c [int]\t', 'Limit the numer of database columns')
    print('-t [float]\t', 'Specify a desired soft dependency threshold tau between 0 and 1 (default 0.7)')
    print('-a [int]\t', 'Specifiy the algorithm to run')
    print('--cords [0|1]\t', 'Use the CORDS pre-filter (1) or not (0)')
    print('\t\t', '0: CORDS pre-filter')
    print('\t\t', '1: (hard) functional dependencies')
    print('\t\t', '2: delta dependencies')
    print('\t\t', '3: soft dependencies')
    print()
    print('OTHER:')
    print('Use \'deltas.txt\' to specifiy per-column delta limits')
    exit(0)



# parse command line arguments
row_limit = -1
col_limit = -1
tau = 0.7
alg = -1
use_cords = False
for i in range(1, len(sys.argv), 2):
    opt = sys.argv[i]
    arg = sys.argv[i + 1]
    # limit rows
    if opt == '-r':
        row_limit = int(arg)
        df = df.limit(row_limit)
    # limit cols
    if opt == '-c':
        col_limit = int(arg)
    if opt == '-a':
        alg = int(arg)
    if opt == '-f':
        tau = int(arg)
    if opt == '--cords':
        use_cords = int(arg) == 1



# load user-specified delta limits
deltas = dict()
with open('deltas.txt') as f:
    for line in f.readlines():
        col_name, delta = line.split(' ')
        deltas[col_name] = int(delta)



# load found dependencies
cords_deps = utils.read_dependencies('./found_deps/cords.json')
found_FDs = utils.read_dependencies('./found_deps/fds.json')
found_DDs = utils.read_dependencies('./found_deps/dds.json')
found_SDs = utils.read_dependencies('./found_deps/sds.json')



# open file for writing output
output_file = open('output.txt', 'a')
output_file.write(f'--- run {datetime.now().date()} {datetime.now().time()} ---\n')



# run specified algorithm
if alg == 0:
    # CORDS pre-filter
    cords.run_CORDS(output_file, spark, df, tau)
elif alg == 1:
    # (hard) functoinal dependencies
    sample_rates = [0.0001, 0.001, 0.005, 0.015, 0.03, 0.1, 0.3]
    found_FDs = hard.find_FDs(output_file, spark, df, lhs_sizes=[1,2,3], sample_rates=sample_rates, col_limit=col_limit)
    utils.write_dependencies('./found_deps/fds.json', found_FDs)
elif alg == 2:
    # delta dependencies
    print('TODO')
elif alg == 3:
    # soft functional dependencies
    cords_output = utils.read_dependencies('./found_deps/cords.json')
    found_SDs = soft.find_SDs(output_file, spark, df, max_lhs_size=3, perc_threshold=tau, found_FDs=found_FDs)
    utils.write_dependencies('./found_deps/sds.json', found_SDs)
else:
    print('Not a valid algorithm specifier')



# close output file
output_file.close()
