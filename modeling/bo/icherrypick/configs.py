offset = "0"
framework = "spark"
threshold = 1.0
config_dir = "configs/"
template = "map.json"
benchmark = "graph/nweight"
num_map = range(2, 34, 2)
memory = {
    'm5.large': 6,
    'm5.xlarge': 14,
    'm5.2xlarge': 29,
    'm5.4xlarge': 60,
    'c5.large': 2,
    'c5.xlarge': 6,
    'c5.2xlarge': 14,
    'c5.4xlarge': 29,
    'r5.large': 14,
    'r5.xlarge': 29,
    'r5.2xlarge': 60,
    'r5.4xlarge': 120,
    'i3.large': 13,
    'i3.xlarge': 28,
    'i3.2xlarge': 59,
    'i3.4xlarge': 120


}
