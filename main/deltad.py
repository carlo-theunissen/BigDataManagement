import utils
import time


def get_flat_map(lhs_size, bv_candidate_DDs, bv_deltas):
    if(lhs_size == 1):
        return lambda row: [((i, bv_deltas.value[dd[1]], row[dd[0]]), (row[dd[1]], row[dd[1]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 2):
        return lambda row: [((i, bv_deltas.value[dd[2]], row[dd[0]], row[dd[1]]), (row[dd[2]], row[dd[2]])) for i, dd in enumerate(bv_candidate_DDs.value)]
    if(lhs_size == 3):
        return lambda row: [((i, bv_deltas.value[dd[3]], row[dd[0]], row[dd[1]], row[dd[2]]), (row[dd[3]], row[dd[3]])) for i, dd in enumerate(bv_candidate_DDs.value)]


def sample_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size, sample_rate):
    if(len(bv_candidate_DDs.value) == 0):
        return bv_candidate_DDs.value

    #Benchmark purpose
    if(sample_rate == 1.0):
        return bv_candidate_DDs.value
    
    rdd = dataframe.rdd
    sampled = rdd.sample(False, sample_rate)
    mapped_DDs = sampled.flatMap(get_flat_map(lhs_size, bv_candidate_DDs, bv_deltas))
    grouped = mapped_DDs.reduceByKey(lambda x, y: (max(x[0], y[0]), min(x[1], y[1])))
    filtered1 = grouped.map(lambda x: (x[0][0], (x[1][0] - x[1][1]) < x[0][1]))
    #data_stripped = filtered1.map(lambda x: (x[0][0], x[1]))
    grouped_by_dd = filtered1.reduceByKey(lambda x, y: x and y)
    #filtered2 = grouped_by_dd.filter(lambda x: x[1])
    bool_stripped = grouped_by_dd.map(lambda x: x[0])
    remaining_DDs = bool_stripped.map(lambda x: bv_candidate_DDs.value[x])
    
    return remaining_DDs.collect()


validate_DDs = lambda dataframe, candidate_DDs, bv_deltas, lhs_size: sample_DDs(dataframe, candidate_DDs, bv_deltas, lhs_size, sample_rate=1.0)


def find_DDs(output_file, spark, dataframe, deltas, lhs_sizes, sample_rates, col_limit = -1):
    lhs_cols = utils.get_col_names(dataframe)
    lhs_cols =  lhs_cols if col_limit == -1 else lhs_cols[:col_limit]
    rhs_cols = utils.get_numeric_col_names(dataframe) + utils.get_timestamp_col_names(dataframe)
    rhs_cols = rhs_cols if col_limit == -1 else rhs_cols[:col_limit]
    
    bv_deltas = spark.sparkContext.broadcast(deltas)

    found_DDs = []
    for lhs_size in lhs_sizes:
        candidate_DDs = utils.generate_deps(lhs_cols, rhs_cols, lhs_size, [cdd for result in found_DDs for cdd in result])
        bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

        for sample_rate in sample_rates:
            output_file.write(f'DD: Running sampele rate {sample_rate} over {len(candidate_DDs)} candidate DDs\n')
            tic = time.perf_counter()

            # find and keep only the remaining candidate_DDs
            candidate_DDs = sample_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size, sample_rate)
            bv_candidate_DDs = spark.sparkContext.broadcast(candidate_DDs)

            toc = time.perf_counter()
            output_file.write(f'DD: Sampling took {toc - tic :0.4f} seconds\n')
        
        validated_DDs = validate_DDs(dataframe, bv_candidate_DDs, bv_deltas, lhs_size)
        print(validate_DDs)
        found_DDs.append(validated_DDs)
    
    return found_DDs
