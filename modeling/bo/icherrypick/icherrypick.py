import math
import numpy as np
import pandas as pd
import json
from helper import *
import configs
import math
import traceback

def constraint(cost, threshold):
    return float(threshold-cost) # returns > 0 as long as cost is below threshold

def evaluate(job_id, params):
    df = pd.read_csv('../../../post-processing/numbers')
    # print df
    cpus = {"large": 2, "xlarge": 4, "2xlarge": 8}
    frac = [0.5, 0.625, 0.75, 0.875, 1.0]
    num = {
        "large": [4, 8, 12, 16],
        "xlarge": [2, 4, 6, 8],
        "2xlarge": [1, 2, 3, 4],
    }
    print params["VM_TYPE"][0]
    print params["VM_SIZE"][0]
    vmtype = params["VM_TYPE"][0]+"."+params["VM_SIZE"][0]
    print vmtype
    num_nodes = num[params["VM_SIZE"][0]][params["NUM"][0]]
    num_partitions = math.ceil(cpus[params["VM_SIZE"][0]] * frac[params["FRAC_PARTITIONS"][0]]) * num_nodes

    runtime = float(df[(df["vmtype"]==vmtype) \
            & (df["number of nodes"]==num_nodes) & (df["number of partitions"] == num_partitions)]["execution time"])
    print runtime
    if runtime == -1.0:
        runtime = 3600
        # runtime = maxRuntime(calculate_cost(config_file), configs.threshold)
    print("Execution time: " + str(runtime))
    # cost = calculate_cost(config_file) * runtime   # y >= x
    # print("Execution cost: " + str(cost))
    # cost_constraint = constraint(cost, configs.threshold)
    return {
        "time" : runtime#,
        # "cost" : cost_constraint
    }

# def evaluate(job_id, params):
#     # print("Optimizing: " + configs.framework)
#     cpu_count = dict()
#     vm_type = dict()
#     num_instances = dict()
#
#     for key in params.keys():
#         print(key.split("-"))
#         framework = key.split("-")[0]
#         param = key.split("-")[1]
#         if param == "CPU_COUNT":
#             cpu_count[framework] = params[key]
#         elif param == "VM_TYPE":
#             vm_type[framework] = params[key]
#         elif param == "NUM":
#             num_instances[framework] = params[key]
#
#     # cpu_count = params['CPU_COUNT']
#     # vm_type = params['VM_TYPE']
#     # num_instances = params['NUM']
#
#     print(params)
#
#     p =dict()
#
#     for key in vm_type.keys():
#         print("Creating the VM type")
#         print(vm_type[key])
#         print(cpu_count[key])
#         p[key.lower()] = dict()
#         p[key.lower()]['type'] = vm_name(vm_type[key], cpu_count[key])
#         print("Framework")
#         print(key.lower())
#         print(p[key.lower()])
#         p[key.lower()]['number'] = configs.num_map[num_instances[key][0]]
#         print(p[key.lower()])
#     config_file = 'config-'+str(job_id+int(configs.offset))+'.json'
#
#     print(config_file)
#     print(configs.template)
#
#     create_json(configs.template, config_file, p)
#
#     # create_json(configs.template, config_file, p, configs.framework)
#     runtime = np.random.randint(100,200)
#     #run_benchmark(config_file, benchmark=configs.benchmark, timeout=maxRuntime(calculate_cost(config_file), configs.threshold))
#
#     # run_benchmark(config_file, benchmark=configs.benchmark, timeout=2400)
#     # runtime = get_runtime()
#     if runtime == 2400:
#         runtime = maxRuntime(calculate_cost(config_file), configs.threshold)
#     print("Execution time: " + str(runtime))
#     cost = calculate_cost(config_file) * runtime   # y >= x
#     print("Execution cost: " + str(cost))
#     cost_constraint = constraint(cost, configs.threshold)
#     return {
#         "time" : runtime,
#         "cost" : cost_constraint
#     }

def main(job_id, params):
    try:
        return evaluate(job_id, params)
    except Exception as ex:
        traceback.print_exc()
        print ex
        print 'An error occurred in icherrypick.py'
        return np.nan
