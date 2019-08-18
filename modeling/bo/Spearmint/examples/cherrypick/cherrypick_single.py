import math
import numpy as np
import json
from helper import *
import configs

def constraint(cost, threshold):
    return float(threshold-cost) # returns > 0 as long as cost is below threshold

def evaluate(job_id, params):
    print("Optimizing: " + configs.framework)
    #cpu_type = params['CPU']
    cpu_count = params['CPU_COUNT']
    vm_type = params['VM_TYPE']
    #ram = params['RAM']
    #disk_type = params['DISK']
    num_instances = params['NUM']

    print(params)

    p =dict()
    p['type'] = vm_name(vm_type,cpu_count)
    print(p['type'])
    #p['type'] = vm_name(cpu_type, cpu_count, ram, disk_type)
    p['number'] = num_instances[0]
    config_file = 'config-'+str(job_id+int(configs.offset))+'.json'
    print(config_file)
    print(configs.template)
    create_json(configs.template, config_file, p, configs.framework)
    #runtime = np.random.randint(100,200)
    runtime = run_benchmark(config_file)
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
        print 'An error occurred in cherrypick_single.py'
        return np.nan
