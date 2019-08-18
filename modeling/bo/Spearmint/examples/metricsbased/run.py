from helper import *
import sys
#import cherrypick_single
from spearmint import main as sp
import fileinput
import configs
import re
from pymongo import MongoClient
import json

frameworks = ["hadoop", "spark", "cassandra"]
prior = None
budget = int(json.load(open('cherrypick/config.json', 'r'))["budget"])
best_val = float("inf")
best_jobid = 1

config_id, val = sp.main(sys.argv[1:])



