from itertools import permutations
import pandas as pd
import numpy as np
import math
from scipy import stats
import utils



def unique_permutations(iterable, r=None):
    previous = tuple()
    for p in permutations(sorted(iterable), r):
        if p > previous:
            previous = p
            yield p



def map_find_sfd_and_pvalue(sampledataset, sfd_threshold, sfd_size = 0.95):
    
    def simple_hash(x, max_value, buckets): 
        return np.floor((x / max_value ) * buckets)
    
    # returns a tuple (dependency, possibleSFD, p-value)
    def _map(dependency):
        import pandas as pd
        lhs, rhs = dependency
        
        # this if statement makes it possible to do some optimalisations
        if len(lhs) > 1:
            # first we construct a column for the left hand side. Combine the individual values
            sampledataset["lhs"] = ""
            for item in lhs:
                sampledataset["lhs"] = sampledataset['lhs'] + "|" + sampledataset[item].apply(str)
            
            # now transform those strings into a number and "normalize" it
            sampledataset['lhs'] = pd.Categorical(sampledataset['lhs'], categories=sampledataset['lhs'].unique()).codes
            sampledataset["lhs"] = sampledataset["lhs"] - sampledataset["lhs"].min()
        else:
            sampledataset['lhs'] = sampledataset[lhs[0]]
        
        
        # now check if there exists a soft function dependency. We use a threshold value (sfd_threshold) and a 
        # size value (sfd_size). The size value is to filter out any soft functional dependecies that look
        # like they are, but are not. See the CORDS paper for more information. 
        new_frame = sampledataset[['lhs',rhs]]
        new_frame = new_frame.drop_duplicates()
        
        out = []
        # number of unique values is the same as the dataset size. Possible Hard dependency otherwise Soft
        if new_frame['lhs'].nunique() >= sfd_threshold * new_frame['lhs'].size \
            and new_frame.shape[0] <= sfd_size * sampledataset.shape[0]:
            
            out.append((dependency, dependency, new_frame['lhs'].nunique() / new_frame['lhs'].size))
        
        # number of unique values is the same as the dataset size. Possible Hard dependency otherwise Soft
        if len(lhs) == 1 and new_frame[rhs].nunique() >= sfd_threshold * new_frame[rhs].size\
            and new_frame.shape[0] <= sfd_size * sampledataset.shape[0]:
            
            out.append( (dependency, ([rhs], lhs[0]), new_frame[rhs].nunique() / new_frame[rhs].size ))

        if len(out) > 0:
            return out

        # the is no soft functional dependency found, therefore calculate the correlation        
        buckets = math.sqrt(new_frame.shape[0] / 5)
        sampledataset["lhs"] = simple_hash(sampledataset["lhs"], sampledataset["lhs"].max(), buckets)
        sampledataset[rhs] = simple_hash(sampledataset[rhs], sampledataset[rhs].max(), buckets)
        
        crosstab =  pd.crosstab(sampledataset['lhs'], sampledataset[rhs])
        _, p, _, _ = stats.chi2_contingency(crosstab)
        
        return [(dependency, None, p)]
    return _map



def filter_based_on_pvalue():
    def _filter(datasettuple):
        return datasettuple[2] < 0.05 or not datasettuple[1] is None
    return _filter



def find_correlected_dependencies_with_spark(spark, dependencies_to_check, transformed_data, tau):
    rdd = spark.sparkContext.parallelize(dependencies_to_check)
    mapped = rdd.flatMap(map_find_sfd_and_pvalue(transformed_data, tau))
    filtered = mapped.filter(filter_based_on_pvalue())
    return filtered.collect()



# TODO: col_limit
def run_CORDS(output_file, spark, dataframe, tau):
    # --- STEP 1 ---

    data_limited = dataframe.limit(16000)
    transformed_data =  data_limited.select("*").toPandas()
    categorical_columns = []

    # first all the string columns are mapped to integers
    for col in transformed_data.select_dtypes(exclude=['number', 'datetime']).columns:
        transformed_data[col] = pd.Categorical(transformed_data[col], categories=transformed_data[col].unique()).codes
        categorical_columns.append(col)

    # secondly, datetime columns are transformed to seconds
    for col in transformed_data.select_dtypes(include=['datetime']).columns:
        transformed_data[col] = transformed_data[col].values.view('<i8')/10**9

    # secondly, datetime columns are transformed to seconds
    for col in transformed_data.columns:
        transformed_data[col] = transformed_data[col] - transformed_data[col].min()
        transformed_data[col] = transformed_data[col].astype('float')

    # --- STEP 2 ---

    # in this list we add all the elements that must be checked by spark
    q1 = []
    q2 = []
    q3 = []

    # We have already seen these combinations
    cachedCombinations = []

    # first we add all the single columns. For instance A->B, B->C BUT NOT B-> A
    for p in unique_permutations(dataframe.columns, 2):
        if (tuple([p[-1]]),p[0:-1]) not in cachedCombinations and len(p[0:-1]) > 0:
            cachedCombinations.append((p[0:-1],tuple([p[-1]])))
            q1.append((list(p[0:-1]),p[-1]))
            
    for p in unique_permutations(dataframe.columns, 3):
            q2.append((list(p[0:-1]),p[-1]))
            
    for p in unique_permutations(dataframe.columns, 4):
            q3.append((list(p[0:-1]),p[-1]))

    # --- FIND SINGLE DEPENDENCIES ---

    single_dependencies = find_correlected_dependencies_with_spark(spark, q1, transformed_data, tau)
    output_file.write(f'CORDS: Found {len(single_dependencies)} single dependencies\n')

    # --- FIND DOUBLE DEPENDENCIES ---

    single_softFDs = list(map(lambda x: (x[1][0], x[1][1]) , filter(lambda x: x[1] is not None, single_dependencies)))
    q2 = list(filter(lambda x: not any(y[1] == x[-1] and set(y[0]).issubset(set(x[0])) for y in single_softFDs), q2))

    # single_correlated is a list with tuples, the 0th index of the tuple is a list for the LHS the
    # 1th index is a single column foro the RHS 
    double_dependencies = find_correlected_dependencies_with_spark(spark, q2, transformed_data, tau)
    output_file.write(f'CORDS: Found {len(double_dependencies)} double dependencies\n')

    # --- FIND TRIPLE DEPENDENCIES ---

    #filter out subsets of already found fds.
    double_softFDs = list(map(lambda x: (x[1][0], x[1][1]) , filter(lambda x: x[1] is not None, double_dependencies)))
    q3 = list(filter(lambda x: not any(y[1] == x[-1] and set(y[0]).issubset(set(x[0])) for y in double_softFDs), q3))

    # single_correlated is a list with tuples, the 0th index of the tuple is a list for the LHS the
    # 1th index is a single column foro the RHS 
    triple_dependencies = find_correlected_dependencies_with_spark(spark, q3, transformed_data, tau)
    output_file.write(f'CORDS: Found {len(triple_dependencies)} triple dependencies\n')

    # --- OUTPUT ---

    to_check_sfd1 = list(map(lambda x: [x[1][0][0], x[1][1]] , filter(lambda x: x[1] is not None, single_dependencies)))
    to_check_sfd2 = list(map(lambda x: [x[1][0][0],x[1][0][1], x[1][1]] , filter(lambda x: x[1] is not None, double_dependencies)))
    to_check_sfd3 = list(map(lambda x: [x[1][0][0],x[1][0][1],x[1][0][2], x[1][1]] , filter(lambda x: x[1] is not None, triple_dependencies)))

    to_check_delta1 = []
    for i in list( filter(lambda x: x[1] is None, single_dependencies)):
        lhs = i[0][0]
        rhs = i[0][1]
        categorical_lhs = lhs[0] in categorical_columns
        categorical_rhs = rhs in categorical_columns
        if not categorical_lhs and not categorical_rhs:
            to_check_delta1.append([lhs[0], rhs])
            to_check_delta1.append([rhs, lhs[0]])
            continue
        if not categorical_lhs:
            to_check_delta1.append([rhs, lhs[0]])
            continue
        if not categorical_rhs:
            to_check_delta1.append([lhs[0], rhs])

    to_check_delta2 = list(
        map(
            lambda x: [x[0][0][0], x[0][0][1], x[0][1]],
            filter(
                lambda x: x[1] is None and not x[0][1] in categorical_columns,
                double_dependencies
            )
        )
    )
    to_check_delta3 = list(
        map(
            lambda x: [x[0][0][0], x[0][0][1], x[0][0][2], x[0][1]],
            filter(
                lambda x: x[1] is None and not x[0][1] in categorical_columns,
                triple_dependencies
            )
        )
    )

    # --- WRITE ---

    res = dict()
    res['to_check_sfd1'] = to_check_sfd1
    res['to_check_sfd2'] = to_check_sfd2
    res['to_check_sfd3'] = to_check_sfd3
    res['to_check_delta1'] = to_check_delta1
    res['to_check_delta2'] = to_check_delta2
    res['to_check_delta3'] = to_check_delta3
    utils.write_dependencies('./found_deps/cords.json', res)
