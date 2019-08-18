import json
import subprocess
import cost
import os
import configs

def run_cmd(cmd, cwd=".", shell=False):
    print cmd
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, cwd=cwd, shell=shell)
    output, error = process.communicate()
    return output

def get_runtime():
    original_cwd = os.getcwd()
    os.chdir(os.getcwd()+"/../")

    resultFile = open("runtime", 'r')
    runtime = float(resultFile.readlines()[-1])
    os.chdir(original_cwd)
    return runtime

def run_benchmark(config_file, parent_dir = 'configs/', benchmark='ml/rf', timeout=1800):
    print "Provision machines and run the benchmark"
    original_cwd = os.getcwd()
    os.chdir(os.getcwd()+"/../")

    cmd = "python provision.py " + parent_dir + config_file + " " + benchmark + " 100 " + str(timeout)

    run_cmd(cmd)

    os.chdir(original_cwd)

# Calculate the execution cost of the configuration
def calculate_cost(config_file, parent_dir = 'configs/'):
    original_cwd = os.getcwd()
    os.chdir(os.getcwd() + "/../")
    params = json.load(open(parent_dir + config_file, 'r'))
    total_cost = 0.0
    for framework in params.keys():
        # print(cost.cost[params[framework]['type']])
        # print(params[framework]['number'])
        total_cost += cost.cost[params[framework]['type']] * (params[framework]['number']/3600.0)
    os.chdir(original_cwd)
    return total_cost

def maxRuntime(cost, threshold):
    return (threshold/cost)+20

def vm_name(vm_type, cpu_count):
    prefix = vm_type[0]
    cpu_count = cpu_count[0]
    suffix=None
    if cpu_count == 1:
        if prefix == "c5":
            suffix = 'xlarge'
        else:
            suffix = 'large'
    elif cpu_count == 2:
        if prefix == "c5":
            suffix = '2xlarge'
        else:
            suffix = 'xlarge'
    elif cpu_count == 8:
        suffix = '2xlarge'
    if suffix is None:
        raise Exception("Invalid VM size.")
    return ".".join([prefix, suffix])

# Determine the VM type based on other values
# def vm_name(cpu_type, cpu_count, ram, disk_type):
#     cpu_type = cpu_type[0]
#     cpu_count = cpu_count[0]
#     ram = ram[0]
#     disk_type = disk_type[0]
#
#     prefix = None
#     print cpu_type, ram, disk_type, cpu_count
#     if cpu_type == 'slow' and ram == 'medium' and disk_type == 'slow':
#         prefix = 'm5'
#     if cpu_type == 'slow' and ram == 'high' and disk_type == 'slow':
#         prefix = 'r4'
#     if cpu_type == 'fast' and ram == 'low' and disk_type == 'slow':
#         prefix = 'c5'
#     if cpu_type == 'slow' and ram == 'high' and disk_type == 'fast':
#         prefix = 'i3'
#     if prefix is None:
#         raise Exception("Invalid VM type.")
#
#     suffix=None
#     if cpu_count == 2:
#         suffix = 'large'
#     elif cpu_count == 4:
#         suffix = 'xlarge'
#     elif cpu_count == 8:
#         suffix = '2xlarge'
#     if suffix is None:
#         raise Exception("Invalid VM size.")
#     return ".".join([prefix, suffix])

# Create a json file with configuration to do a single run of the experiment
# TODO: This is different that with one framework at a time
def create_json(template, filename, params, framework="hadoop", parent_dir = '../configs/'):
    print(os.getcwd())
    if template == 'map.json':
        temp = json.load(open('../' + template, 'r'))
    else:
        temp = json.load(open('../'+ template, 'r'))
    print(params)
    print(temp)
    for fm in params.keys():
        for key in params[fm].keys():
            if key == "number":
                temp[fm]["number"] = params[fm]["number"]
            if key == "type":
                temp[fm]["type"] = params[fm]["type"]
                temp[fm]["memory"] = str(configs.memory[params[fm]["type"]]) + 'G'
    print(temp)
    fp = open(parent_dir + filename, 'w')
    json.dump(temp, fp)
    fp.close()
