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
offset = 0
for framework in frameworks:

    r = re.compile(r"(framework\s=\s).*")
    for line in fileinput.input('cherrypick/configs.py', inplace = True):
        print r.sub(r"\1'%s'" %framework , line),
        #print line.replace("framework = .*", "framework = \"" + framework + "\""),v
    fileinput.close()
    #run_cmd("sed -i \'\' \"s/framework = .*/framework = \'"+framework+"\'/g\" cherrypick/configs.py")
    #configs.framework = framework
    config_id, val = sp.main(sys.argv[1:])
    config_id = int(config_id) + 1 + int(offset)
    print(offset)
    print(config_id)
    val = int(val)

    # TODO: May be take cost into consideration while breaking ties
    if val <= best_val:
        print("Current best is now better than previous so using id: " + str(config_id) + " with val: " + str(val))
        best_val = val
        best_jobid = config_id
    else:
        print("Current best is NOT better than previous so using id: " + str(best_jobid) + " with val: " + str(best_val))


    prior = 'configs/config-'+str(best_jobid)+'.json'
    r = re.compile(r"(template\s=\s).*")
    for line in fileinput.input('cherrypick/configs.py', inplace = True):
        print r.sub(r"\1'%s'" %prior , line),
        #print line.replace("framework = .*", "framework = \"" + framework + "\""),
    fileinput.close()
    run_cmd("rm output/*")

    client = MongoClient('127.0.0.1')
    db = client["spearmint"]
    db.drop_collection(json.load(open('cherrypick/config.json', 'r'))["experiment-name"]+".jobs")
    db.drop_collection(json.load(open('cherrypick/config.json', 'r'))["experiment-name"] + ".hypers")
    #client.drop_database('spearmint')

    r = re.compile(r"(offset\s=\s).*")
    offset = (frameworks.index(framework) + 1 ) * budget
    print("Set offset to " + str(offset))
    for line in fileinput.input('cherrypick/configs.py', inplace = True):
        print r.sub(r"\1'%s'"  %str(offset) , line),
        #print line.replace("framework = .*", "framework = \"" + framework + "\""),
    fileinput.close()


