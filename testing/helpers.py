import subprocess
import os
import time
import json
import paramiko
import sys
from scp import SCPClient

def start(client, output="/tmp/sar.dat", interval=1):
    client.exec_command("rm -f".format(output))
    client.exec_command("mkdir -p {}".format(os.path.dirname(output)))
    cmd_start = "nohup sar -p -A -o {} {} > /dev/null 2>&1 &".format(output, interval)
    # print(cmd_start)
    client.exec_command(cmd_start)

def stop(client):
    cmd_kill = "pkill -x sar"
    client.exec_command(cmd_kill)

def export(client, input="/tmp/sar.dat", output="/tmp/sar.csv", interval=1):
    client.exec_command('mkdir -p {}'.format(os.path.dirname(output)))
    # TODO: this tool does not support multiple devices
    cmd_general = 'sadf -dh -t {} {} -- -p -bBqSwW -u ALL -I SUM -r ALL | csvcut -d ";" -C "# hostname,interval,CPU,INTR" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_general.csv")
    cmd_disk = 'sadf -dh -t {} {} -- -p -d | csvcut -d ";" -C "# hostname,interval,DEV" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_disk.csv")
    cmd_network = 'sadf -dh -t {} {} -- -p -n DEV | csvcut -d ";" -C "# hostname,interval,IFACE" | sed "1s/\[\.\.\.\]//g" > {}'.format(interval, input, "/tmp/sar_network.csv")
    cmd_join = 'csvjoin -c timestamp /tmp/sar_general.csv /tmp/sar_disk.csv | csvjoin -c timestamp - /tmp/sar_network.csv | csvformat -u 3 > {}'.format("/tmp/sar.csv")
#    modified_header = 'timestamp,cpu.%usr,cpu.%nice,cpu.%sys,cpu.%iowait,cpu.%steal,cpu.%irq,cpu.%soft,cpu.%guest,cpu.%gnice,cpu.%idle,task.proc/s,task.cswch/s,intr.intr/s,swap.pswpin/s,swap.pswpout/s,paging.pgpgin/s,paging.pgpgout/s,paging.fault/s,paging.majflt/s,paging.pgfree/s,paging.pgscank/s,paging.pgscand/s,paging.pgsteal/s,paging.%vmeff,io.tps,io.rtps,io.wtps,io.bread/s,io.bwrtn/s,memory.kbmemfree,memory.kbavail,memory.kbmemused,memory.%memused,memory.kbbuffers,memory.kbcached,memory.kbcommit,memory.%commit,memory.kbactive,memory.kbinact,memory.kbdirty,memory.kbanonpg,memory.kbslab,memory.kbkstack,memory.kbpgtbl,memory.kbvmused,swap.kbswpfree,swap.kbswpused,swap.%swpused,swap.kbswpcad,swap.%swpcad,load.runq-sz,load.plist-sz,load.ldavg-1,load.ldavg-5,load.ldavg-15,load.blocked,disk.tps,disk.rd_sec/s,disk.wr_sec/s,disk.avgrq-sz,disk.avgqu-sz,disk.await,disk.svctm,disk.%util,network.rxpck/s,network.txpck/s,network.rxkB/s,network.txkB/s,network.rxcmp/s,network.txcmp/s,network.rxmcst/s,network.%ifutil'
#    cmd_create = 'echo {} > {}'.format(modified_header, output)
#    cmd_header = 'tail -n +2 {} >> {}'.format("/tmp/sar_join.csv", output)
    client.exec_command(cmd_general)
    client.exec_command(cmd_disk)
    client.exec_command(cmd_network)
    client.exec_command(cmd_join)
#    client.exec_command(cmd_create)
#    client.exec_command(cmd_header)

def getfiles(scp, index, dir, parentDir='sar_logs/'):
    if not os.path.exists(parentDir+dir):
        os.makedirs(parentDir+dir)
    print(dir)
    scp.get('/tmp/sar.csv', parentDir+dir+"/"+"node_"+index+".csv")
