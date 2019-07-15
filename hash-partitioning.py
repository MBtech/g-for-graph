import sys

def partitioner(edge, num_partition, membership):
    partition = abs(hash(hash(edge[0]) + hash(edge[1]))) % num_partition
    membership[partition].add(edge[0])
    membership[partition].add(edge[1])
    return partition

graph_file = sys.argv[1]
num_partition = int(sys.argv[2])
membership = [set() for i in range(0, num_partition)]
partition_size = [0 for i in range(0 , num_partition)]
original = set()

with open(graph_file, 'r') as f:
    for edge in f:
        edge = edge.strip('\n').split()
        # print edges

        original.add(edge[0])
        original.add(edge[1])
        p = partitioner(edge, num_partition, membership)
        partition_size[p] +=1

replicated = sum([len(partition) for partition in membership])
print len(original)
print replicated
