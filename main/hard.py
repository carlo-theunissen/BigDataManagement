import utils
import time



def get_flat_map(lhs_size, bv_candidate_FDs):
    if(lhs_size == 1):
        return lambda row: [((i, row[fd[0]]), (row[fd[1]], True)) for i, fd in bv_candidate_FDs.value]
    if(lhs_size == 2):
        return lambda row: [((i, row[fd[0]], row[fd[1]]), (row[fd[2]], True)) for i, fd in bv_candidate_FDs.value]
    if(lhs_size == 3):
        return lambda row: [((i, row[fd[0]], row[fd[1]], row[fd[2]]), (row[fd[3]], True)) for i, fd in bv_candidate_FDs.value]



def sample_FDs(dataframe, bv_candidate_FDs, lhs_size, sample_rate):
    if(len(bv_candidate_FDs.value) == 0):
        return bv_candidate_FDs
    
    rdd = dataframe.rdd
    sample = rdd.sample(False, sample_rate)
    mapped_FDs = sample.flatMap(get_flat_map(lhs_size, bv_candidate_FDs))
    grouped = mapped_FDs.reduceByKey(lambda x, y: y if(x == y) else (x[0], False))
    data_stripped = grouped.map(lambda x: (x[0][0], x[1][1]))
    grouped_by_fd = data_stripped.reduceByKey(lambda x, y: x and y)
    filtered = grouped_by_fd.filter(lambda x: x[1])
    bool_stripped = filtered.map(lambda x: x[0])
    remaining_FDs = bool_stripped.map(lambda x: bv_candidate_FDs.value[x])
    
    return remaining_FDs.collect()

validate_FDs = lambda dataframe, bv_candidate_FDs, lhs_size: sample_FDs(dataframe, bv_candidate_FDs, lhs_size, sample_rate=1.0)



# TODO: use batching?
def find_FDs(output_file, spark, dataframe, lhs_sizes, sample_rates, colLimit = -1):
    col_names = utils.get_all_cols(dataframe)
    col_names =  col_names if colLimit == -1 else col_names[:colLimit]

    found_FDs = []
    for lhs_size in lhs_sizes:
        candidate_FDs = utils.generate_deps(col_names, col_names, lhs_size, found_FDs)
        bv_candidate_FDs = spark.sparkContext.broadcast(candidate_FDs)

        for sample_rate in sample_rates:
            output_file.write(f'Running sampling rate {sample_rate} over {len(candidate_FDs)} candidate FDs\n')
            tic = time.perf_counter()

            # find and keep only the remaining candidate_FDs
            candidate_FDs = sample_FDs(dataframe, bv_candidate_FDs, lhs_size, sample_rate)

            toc = time.perf_counter()
            output_file.write(f'Sampling took {toc - tic :0.4f} seconds\n')
        
        validated_FDs = validate_FDs(dataframe, bv_candidate_FDs, lhs_size)
        found_FDs.append(validated_FDs)

    return found_FDs