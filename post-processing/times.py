import sys
from datetime import datetime
import glob
import pandas as pd

def get_numbers(filename):
    times = list()
    f = open(filename, 'r')
    lines = f.readlines()

    datetime_load = None
    datetime_loaded = None
    datetime_partitioned = None

    for line in lines:
        if "Loading edge list" in line:
            datetime_str = line.split(" ")[0].strip('\n')
            datetime_load = datetime.strptime(datetime_str, '%Y-%m-%d_%H:%M:%S')
        if "Graph Loaded" in line:
            datetime_str = line.split(" ")[0].strip('\n')
            datetime_loaded = datetime.strptime(datetime_str, '%Y-%m-%d_%H:%M:%S')
        if "Partitioning Done" in line:
            line.split(" ")[0].strip('\n')
            datetime_str = line.split(" ")[0].strip('\n')
            datetime_partitioned = datetime.strptime(datetime_str, '%Y-%m-%d_%H:%M:%S')

    if datetime_loaded != None:
        times.append((datetime_loaded - datetime_load).total_seconds())
        print("Time taken to load graph: " + \
                str(times[0]))
    else:
        times.append(-1.0)
        print("Time taken to load graph: -1")

    if datetime_partitioned != None:
        times.append((datetime_partitioned - datetime_loaded).total_seconds())
        print("Time taken to repartition the graph: " + \
                str(times[1]))
    else:
        times.append(-1.0)
        print("Time taken to repartition the graph: -1")

    if times[0] == -1.0 or times[1] == -1.0:
        times.append(-1.0)
    else:
        times.append(float(lines[-1].strip('\n')))
    print("Execution time: " + str(times[2]))
    return times

parent_dir="../logs/"
dir="PageRank-datagen-8_4-fb.e/"
filename = "PageRank_c5.2xlarge_3_12.log"

files = [f for f in glob.glob(parent_dir+dir + "*.log")]
print files

data = list()
for file in files:
    filename = file.split("/")[-1]
    vmtype = filename.split('_')[1]
    n_nodes = filename.split('_')[2]
    n_partitions = filename.split('_')[3].split('.')[0]
    numbers = get_numbers(file)
    d = list()

    d.extend([vmtype, n_nodes, n_partitions])
    d.extend(numbers)
    print d
    # print [vmtype, n_nodes, n_partitions].extend(numbers)
    # data.append(list([vmtype, n_nodes, n_partitions].extend(numbers)))
    data.append(d)

print data
df = pd.DataFrame(data, columns=["vmtype", "number of nodes", "number of partitions"\
        , "load time", "partition time", "execution time"])
print df
feasible_data = df[df['execution time'] != -1]
print feasible_data.sort_values(by=['execution time'])
print feasible_data.shape
df.to_csv('numbers', sep=',', index=False)
# new_df = pd.read_csv('numbers')
# print new_df
