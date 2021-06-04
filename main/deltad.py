import utils
import time


def get_flat_map(lhs_size, bv_candidate_DDs, deltas):
    if(lhs_size == 1):
        return lambda row: [((i, deltas[dd[1]], row[dd[0]]), (row[dd[1]], row[dd[1]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 2):
        return lambda row: [((i, deltas[dd[2]], row[dd[0]], row[dd[1]]), (row[dd[2]], row[dd[2]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 3):
        return lambda row: [((i, deltas[dd[3]], row[dd[0]], row[dd[1]], row[dd[2]]), (row[dd[3]], row[dd[3]])) for i, dd in enumerate(bv_candidate_DDs.value)]


def sample_DDs(dataframe, bv_candidate_DDs, deltas, lhs_size, sample_rate):
    if(len(bv_candidate_DDs.value) == 0):
        return bv_candidate_DDs.value

    #Benchmark purpose
    if(sample_rate == 1.0):
        return bv_candidate_DDs.value
    
    rdd = dataframe.rdd
    sampled = rdd.sample(False, sample_rate)
    mapped_DDs = sampled.flatMap(get_flat_map(lhs_size, bv_candidate_DDs, deltas))
    grouped = mapped_DDs.reduceByKey(lambda x, y: (max(x[0], y[0]), min(x[1], y[1])))
    filtered1 = grouped.map(lambda x: (x[0][0], (x[1][0] - x[1][1]) < x[0][1]))
    #data_stripped = filtered1.map(lambda x: (x[0][0], x[1]))
    grouped_by_dd = filtered1.reduceByKey(lambda x, y: x and y)
    #filtered2 = grouped_by_dd.filter(lambda x: x[1])
    bool_stripped = grouped_by_dd.map(lambda x: x[0])
    remaining_DDs = bool_stripped.map(lambda x: bv_candidate_DDs.value[x])
    
    return remaining_DDs.collect()


validate_DDs = lambda dataframe, candidate_DDs, deltas, lhs_size: sample_DDs(dataframe, candidate_DDs, deltas, lhs_size, sample_rate=1.0)


def find_DDs(spark, dataframe, deltas, lhs_sizes, sample_rates, col_limit = -1):
    all_cols = utils.get_col_names(dataframe)
    all_cols =  all_cols if col_limit == -1 else all_cols[:col_limit]
    numeric_cols = utils.get_numeric_col_names(dataframe)
    numeric_cols = numeric_cols if col_limit == -1 else numeric_cols[:col_limit]

    found_DDs = []
    for lhs_size in lhs_sizes:
        candidate_DDs = utils.generate_deps(all_cols, numeric_cols, lhs_size, found_DDs)
        bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

        for sample_rate in sample_rates:
            print(f'Running sampele rate {sample_rate} over {len(candidate_DDs)} candidate _DDs')
            tic = time.perf_counter()

            # find and keep only the remaining candidate_DDs
            candidate_DDs = sample_DDs(dataframe, bv_candidate_DDs, deltas, lhs_size, sample_rate)
            bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

            toc = time.perf_counter()
            print(f'Sampling took {toc - tic :0.4f} seconds')
        
        validated_DDs = validate_DDs(dataframe, bv_candidate_DDs, deltas, lhs_size)
        found_DDs.append(validated_DDs)
    
    return found_DDs
