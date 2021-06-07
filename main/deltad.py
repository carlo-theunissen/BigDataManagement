import utils
import time


def get_flat_map(lhs_size, bv_candidate_DDs):
    if(lhs_size == 1):
        return lambda row: [((i, row[dd[0]]), (row[dd[1]], row[dd[1]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 2):
        return lambda row: [((i, row[dd[0]], row[dd[1]]), (row[dd[2]], row[dd[2]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 3):
        return lambda row: [((i, row[dd[0]], row[dd[1]], row[dd[2]]), (row[dd[3]], row[dd[3]])) for i, dd in enumerate(bv_candidate_DDs.value)]


def sample_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size, sample_rate):
    if(len(bv_candidate_DDs.value) == 0):
        return bv_candidate_DDs.value

    #Benchmark purpose
    #if(sample_rate == 1.0):
    #    return bv_candidate_DDs.value

    if(len(bv_candidate_DDs.value) <= 10 and sample_rate != 1.0):
        return bv_candidate_DDs.value

    rdd = dataframe.rdd
    sampled = rdd.sample(False, sample_rate)
    mapped_DDs = sampled.flatMap(get_flat_map(lhs_size, bv_candidate_DDs))
    grouped = mapped_DDs.reduceByKey(lambda x, y: (max(x[0], y[0]), min(x[1], y[1])))
    filtered1 = grouped.map(lambda x: (x[0][0], (x[1][0] - x[1][1]) < bv_deltas.value[ bv_candidate_DDs.value[x[0][0]][-1] ]))
    #data_stripped = filtered1.map(lambda x: (x[0][0], x[1]))
    grouped_by_dd = filtered1.reduceByKey(lambda x, y: x and y)
    filtered2 = grouped_by_dd.filter(lambda x: x[1])
    bool_stripped = filtered2.map(lambda x: x[0])
    remaining_DDs = bool_stripped.map(lambda x: bv_candidate_DDs.value[x])
    
    return remaining_DDs.collect()


validate_DDs = lambda dataframe, candidate_DDs, bv_deltas, lhs_size: sample_DDs(dataframe, candidate_DDs, bv_deltas, lhs_size, sample_rate=1.0)


def find_DDs(output_file, spark, dataframe, deltas, lhs_sizes, sample_rates, found_FDs, use_CORDS = False, col_limit = None):
    lhs_cols = utils.get_col_names(dataframe)
    lhs_cols =  lhs_cols if col_limit == None else lhs_cols[:col_limit]
    rhs_cols = utils.get_numeric_col_names(dataframe) + utils.get_timestamp_col_names(dataframe)
    rhs_cols = rhs_cols if col_limit == None else rhs_cols[:col_limit]
    
    bv_deltas = spark.sparkContext.broadcast(deltas)

    cords = utils.read_dependencies('./found_deps/cords.json')

    found_DDs = []
    for lhs_size in lhs_sizes:
        # get and broadcast dependencies for current LHS size
        candidate_DDs = []
        if use_CORDS:
            candidate_DDs = cords[f'to_check_delta{lhs_size}']
        else:
            ignored_DDs = found_FDs + found_DDs
            candidate_DDs = utils.generate_deps(lhs_cols, rhs_cols, lhs_size, ignored_DDs)
        bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

        # apply progressive sampling
        for sample_rate in sample_rates:
            output_file.write(f'DD: Running sampele rate {sample_rate} over {len(candidate_DDs)} candidate DDs\n')
            print(f'DD: Running sampele rate {sample_rate} over {len(candidate_DDs)} candidate DDs\n')
            tic = time.perf_counter()

            # find and keep only the remaining candidate_DDs
            candidate_DDs = sample_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size, sample_rate)
            bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

            toc = time.perf_counter()
            output_file.write(f'DD: Sampling took {toc - tic :0.4f} seconds\n')
            print(f'DD: Sampling took {toc - tic :0.4f} seconds\n')
        
        # validate remaining candidate DDs
        validated_DDs = validate_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size)
        print(list(validated_DDs))
        found_DDs += validated_DDs
    
    return found_DDs
