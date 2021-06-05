from pyspark.sql import SparkSession
import sys
import utils
import hard, soft, deltad, cords
from datetime import datetime, timedelta
import argparse



# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('-r', '--rows', type=int, help='limit the number of database rows')
parser.add_argument('-c', '--columns', type=int, help='limit the number of database columns')
parser.add_argument('-t', '--tau', type=float, default=0.7, help='set the soft dependency threshold tau')
parser.add_argument('-a', '--alg', type=int, choices=[0, 1, 2, 3], help='run CORDS, HFDs, DDs, or SFDs')
parser.add_argument('--cords', help='use the CORDS pre-filter results', action='store_true')
args = parser.parse_args()



# connect to spark and read data
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('preprocessed_data.csv', header=True, inferSchema=True, mode='PERMISSIVE', encoding='ISO-8859-1')
if not args.rows == None:
    df = df.limit(args.rows)



# load user-specified delta limits
deltas = dict()
with open('deltas.txt') as f:
    dtypes = dict(df.dtypes)
    for line in f.readlines():
        col_name, delta = line.split(' ')
        if dtypes[col_name] == 'timestamp':
            deltas[col_name] = timedelta(minutes=int(delta))
        else:
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
if args.alg == 0:
    # CORDS pre-filter
    cords.run_CORDS(
        output_file,
        spark,
        df,
        tau=args.tau
    )
elif args.alg == 1:
    # (hard) functoinal dependencies
    sample_rates = [0.0001, 0.001, 0.005, 0.015, 0.03, 0.1, 0.3]
    found_FDs = hard.find_FDs(
        output_file,
        spark,
        df,
        lhs_sizes=[1,2,3],
        sample_rates=sample_rates,
        col_limit=args.columns
    )
    utils.write_dependencies('./found_deps/fds.json', found_FDs)
elif args.alg == 2:
    # delta dependencies
    sample_rates = [0.0001, 0.001, 0.005, 0.015, 0.03, 0.1, 0.3]
    found_DDs = deltad.find_DDs(
        output_file,
        spark,
        df,
        deltas,
        lhs_sizes=[1,2,3],
        sample_rates=sample_rates,
        found_FDs=found_FDs,
        use_CORDS=args.cords,
        col_limit=args.columns
    )
    utils.write_dependencies('./found_deps/dds.json', found_DDs)
elif args.alg == 3:
    # soft functional dependencies
    found_SDs = soft.find_SDs(
        output_file,
        spark,
        df,
        max_lhs_size=3,
        perc_threshold=args.tau,
        found_FDs=found_FDs,
        use_CORDS=args.cords,
        col_limit=args.columns
    )
    utils.write_dependencies('./found_deps/sds.json', found_SDs)
else:
    print('Not a valid algorithm specifier')



# close output file
output_file.close()
