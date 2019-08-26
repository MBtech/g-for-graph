import os
import json
import paramiko
import sys
from scp import SCPClient
from helpers import *
# from multiprocessing import Pool
# from itertools import repeat
#
# def sar_helper(ip, framework, count, confFile):
#     print(ip, framework, count, confFile)
#     client = paramiko.SSHClient()
#     client.load_system_host_keys()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     #client.set_missing_host_key_policy(paramiko.WarningPolicy())
#     client.connect(ip, username="ubuntu", password="", pkey=None, key_filename="/Users/mb/Downloads/bilal-us-east.pem.txt")
#
#     # SCPCLient takes a paramiko transport as an argument
#     scp = SCPClient(client.get_transport())
#
#     scp.close()
#     if sys.argv[1]=="start":
#         start(client)
#     elif sys.argv[1]=="terminate":
#         stop(client)
#     elif sys.argv[1]=="collectcsv":
#         getfiles(scp, framework, str(count), confFile)
#     else:
#         export(client)

confFile = sys.argv[2]
f = open(confFile, 'r')
ips = [line.strip('\n') for line in f.readlines()]
count = 0
print ips
for ip in ips:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect(ip, username="ubuntu", password="", pkey=None)

    # SCPCLient takes a paramiko transport as an argument
    scp = SCPClient(client.get_transport())

    scp.close()
    if sys.argv[1]=="start":
        start(client)
    elif sys.argv[1]=="terminate":
        stop(client)
    elif sys.argv[1]=="collectcsv":
        getfiles(scp, str(count), sys.argv[3])
    else:
        export(client)

    count +=1
