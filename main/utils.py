import json
from itertools import product, combinations



# read found dependencies
def read_dependencies(path):
    with open(path, 'r') as f:
        return json.load(f)

# write found dependencies
def write_dependencies(path, deps):
    with open(path, 'w') as f:
        json.dump(deps, f)



get_dtype_name = lambda dtype: dtype[0]
get_col_names = lambda dataframe: list(map(get_dtype_name, dataframe.dtypes))

is_numeric_dtype = lambda dtype: dtype[1] == 'int'
get_numeric_col_names = lambda dataframe: list(map(get_dtype_name, filter(is_numeric_dtype, dataframe.dtypes)))

filter_deps = lambda candidateDeps, found_deps: list(filter(lambda dep: not dep in found_deps, candidateDeps))



def generate_deps(lhs_columns, rhs_columns, lhsSize: int, alreadyFound = []):
    # generate left hand sides
    lhss = combinations(lhs_columns, r=lhsSize)
    # add right hand sides
    deps = product(lhss, rhs_columns)
    # convert to list format, where ['a', 'b', 'c'] corresponds to 'a', 'b' -> 'c'
    deps = map(lambda dep: list(dep[0]) + [dep[1]], deps)

    # remove tirivial dependencies (where RHS in LHS)
    deps = filter(lambda dep: not dep[-1] in dep[:-1], deps)

    # used to test if dependency is implied by some found dependency
    includes_lhs = lambda found_dep, dep: all(attr in dep[:-1] for attr in found_dep[:-1])
    implied = lambda dep: any(map(lambda found_dep: found_dep[-1] == dep[-1] and includes_lhs(found_dep, dep), alreadyFound))
    # remove already found dependencies
    deps = filter(lambda dep: not implied(dep), deps)

    return list(deps)



def print_dependencies(dependencies):
    for dep in sorted(dependencies):
        lhs = dep[:-1]
        rhs = dep[-1]
        print(', '.join(lhs), '->', rhs)
