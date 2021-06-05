from math import factorial
from itertools import combinations, product
import time
import utils



# TODO: check lhs_size
def gen_contin_cells(x, bv_candidate_SDs):
    combs = []
    candidate_FDs = [tuple(candidate_FD) for candidate_FD in bv_candidate_SDs.value]
    #     for comb in candidate_FDs:
    for i, fd in enumerate(candidate_FDs):
    # rows in form: (((lhs, rhs), (value(s) lhs, value rhs)), count)
        combs.append(((i, tuple(x[column] for column in fd)),1))
    return combs



def calc_combinations(x):
    #     combinations of 2
    return factorial(x)/(factorial(2)*factorial(x-2)) if x >= 2 else 0



def map_value_to_combs_count(rdd_row):
    lhs_plus_rhs = rdd_row[0][0]
    #     value of rhs is always last item. Strip it to only depend on value of lhs in key
    values_lhs_tup = rdd_row[0][1][:-1]
    reduced_count_rows = rdd_row[1]
    return ((lhs_plus_rhs, values_lhs_tup), calc_combinations(reduced_count_rows))



def calc_percentage(x, y):
    #     normalize to percentage value in [0, 1]
    result = y/x if x >= y else x/y
    #     if normalized percentage value = 1 then set to 1.5 instead so this value cannot be confused
    #     with a possible total 2 comb count of 1 for lhs
    #     as total combination count always has to be integer
    result = 1.5 if result == 1.0 else result 
    return result



def map_total_comb_to_zero_perc(rdd_row):
    lhs_plus_rhs = rdd_row[0][0] 
    value = rdd_row[1]
    #     rdd_row could be row with total 2 comb count per lhs as value that did not get normalized to [0, 1] range as percentage
    #     as all rows for that lhs had unique rhs values so there was no row with possible combinations of 2 equal lhs + rhs per lhs
    #     to reduce with. Rows with unique lhs + rhs were filtered in step #3. 
        #     lhs that has no possible combinations of 2 equal lhs + rhs per lhs will be set to 0%
    #     otherwise rdd_row has normalized percentage as value but in the case of percentage being 100% value = 1.5 instead of 1.
    if value >= 1 and value != 1.5:
        value = 0
    elif value == 1.5: # set 100%'s back to 1
        value = 1
    return (lhs_plus_rhs, value)
    


def rem_rhs_value_from_key(rdd_row):
    lhs_plus_rhs = rdd_row[0][0]
    #     value of rhs is always last item. Strip it to only depend on value of lhs in key
    values_lhs_tup = rdd_row[0][1][:-1]
    value = rdd_row[1]
    return ((lhs_plus_rhs, values_lhs_tup), value)



def find_SDs(output_file, spark, dataframe, max_lhs_size = 3, perc_threshold = 0.7, found_FDs = [], use_CORDS = False, col_limit = None):
    col_names = utils.get_col_names(dataframe)
    col_names = col_names if col_limit == None else col_names[:col_limit]

    cords = utils.read_dependencies('./found_deps/cords.json')

    found_SDs = []
    for lhs_size in range(1, max_lhs_size + 1):
        # get and broadcast dependencies for current LHS size
        # http://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables
        candidate_SDs = []
        if use_CORDS:
            candidate_SDs = cords[f'to_check_sfd{lhs_size}']
        else:
            ignored_SDs = found_FDs + found_SDs
            candidate_SDs = utils.generate_deps(col_names, col_names, lhs_size, ignored_SDs)
        broadcast_candidate_SDs = spark.sparkContext.broadcast(candidate_SDs)

        tic = time.perf_counter()

        candidate_SDs = utils.generate_deps(col_names, col_names, lhs_size, found_FDs)
        
        output_file.write(f"Running full dataset over {len(candidate_SDs)} possible Soft Dependencies with threshold: {perc_threshold} and lhs: {lhs_size}\n")
        # #    sample only for local use to test
        #     flat_columns = cont_sample_data.rdd.flatMap(lambda x: gen_contin_cells(x, broadcast_candidate_FDs, lhs_size=i)) # 1

        #     create all column combs per row
        flat_columns = dataframe.rdd.flatMap(lambda x: gen_contin_cells(x, broadcast_candidate_SDs)) # 1
        #     cache or not?? only used one time extra this rdd later? check if this actually wins time
        c_flat_columns = flat_columns.reduceByKey(lambda x,y: x+y).cache() # 2
        #     unique lhs + rhs rows are not needed to calculate possible combinations of 2 rows with equal lhs.
        f_c_flat_columns = c_flat_columns.filter(lambda x: x[1] >= 2) # 3
        #     map identical lhs + rhs occurences to possible combs of 2
        calc_combs = f_c_flat_columns.map(lambda x: map_value_to_combs_count(x)) # 4
        #     reduce amount of possible combinations of 2 equal lhs + rhs per lhs
        reduce_combs_by_lhs = calc_combs.reduceByKey(lambda x,y: x+y) # 5
        
        #     make use of already cached reduced rdd from step #2
        #     make sure to only reduce on lhs as we want total 2 comb count for lhs
        row_c_for_lhs = c_flat_columns.map(lambda x: rem_rhs_value_from_key(x))
        red_c_for_lhs = row_c_for_lhs.reduceByKey(lambda x,y: x+y)
        #     Filter out rows with unique lhs as they cannot match with another equal lhs row
        filt_red_c_for_lhs = red_c_for_lhs.filter(lambda x: x[1] >= 2)
        #     calculate total number of 2 row combs per lhs
        map_total_combs_lhs = filt_red_c_for_lhs.mapValues(lambda value: calc_combinations(value))
        
        #     now union the per (lhs, rhs) comb count rdd and per (lhs) comb count rdd
        total_and_eq_combs = map_total_combs_lhs.union(reduce_combs_by_lhs)# 8
        #     calc percentage per lhs per FD
        #     this could be bottleneck as now every key only has 2 rows so low chance of being able to combine locally?
        percentage_per_lhs = total_and_eq_combs.reduceByKey(lambda x,y: calc_percentage(x,y)) # 9
        
        mapped_percentages = percentage_per_lhs.map(lambda x: map_total_comb_to_zero_perc(x))
        #     reduce by value (percentage) per lhs. Keep separate count to calculate the mean over the percentages of one FD combination (mapValues).
        means_percentages = mapped_percentages.mapValues(lambda value: (value, 1)) \
                                                .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])) \
                                                .mapValues(lambda value: value[0]/value[1])
        
        filter_threshold_fds = means_percentages.filter(lambda x: x[1] >= perc_threshold)
        #     take only fd index and map index to full fd
        take_only_fd_indic = filter_threshold_fds.map(lambda x: broadcast_candidate_SDs.value[x[0]])
        soft_fd_comb_list = take_only_fd_indic.collect()
        #     form: [["lhs column", "lhs column", "rhs column"], ....] depending on value of i in loop for amount of lhs columns
        found_SDs += soft_fd_comb_list
        
        toc = time.perf_counter()
        output_file.write(f"Discovering SDs took {toc - tic:0.4f} seconds\n")
        output_file.write(f'Number of dependencies found: {len(found_SDs)}\n')
    
    return found_SDs
