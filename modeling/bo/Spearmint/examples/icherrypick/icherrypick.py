import math
import numpy as np
import json
from helper import *
import configs

def constraint(cost, threshold):
    return float(threshold-cost) # returns > 0 as long as cost is below threshold

def evaluate(job_id, params):
    print("Optimizing: " + configs.framework)
    cpu_count = dict()
    vm_type = dict()
    num_instances = dict()

    for key in params.keys():
        print(key.split("-"))
        framework = key.split("-")[0]
        param = key.split("-")[1]
        if param == "CPU_COUNT":
            cpu_count[framework] = params[key]
        elif param == "VM_TYPE":
            vm_type[framework] = params[key]
        elif param == "NUM":
            num_instances[framework] = params[key]

    # cpu_count = params['CPU_COUNT']
    # vm_type = params['VM_TYPE']
    # num_instances = params['NUM']

    print(params)

    p =dict()

    for key in vm_type.keys():
        print("Creating the VM type")
        print(vm_type[key])
        print(cpu_count[key])
        p[key.lower()] = dict()
        p[key.lower()]['type'] = vm_name(vm_type[key], cpu_count[key])
        print("Framework")
        print(key.lower())
        print(p[key.lower()])
        p[key.lower()]['number'] = num_instances[key][0]
        print(p[key.lower()])
    config_file = 'config-'+str(job_id+int(configs.offset))+'.json'

    print(config_file)
    print(configs.template)

    create_json(configs.template, config_file, p)

    #create_json(configs.template, config_file, p, configs.framework)
    #runtime = np.random.randint(100,200)

    runtime = run_benchmark(config_file)
    runtime = get_runtime()
    print("Execution time: " + str(runtime))
    cost = calculate_cost(config_file) * runtime   # y >= x
    print("Execution cost: " + str(cost))
    cost_constraint = constraint(cost, configs.threshold)
    return {
        "time" : runtime,
        "cost" : cost_constraint
    }

def main(job_id, params):
    try:
        return evaluate(job_id, params)
    except Exception as ex:
        print ex
        print 'An error occurred in icherrypick.py'
        return np.nan
