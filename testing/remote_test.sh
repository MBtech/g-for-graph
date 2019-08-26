#!/bin/bash
source configs
scp -i ~/Downloads/bilal_key.pem -r ../testing ubuntu@${MASTER_IP}:~/
ssh -i ~/Downloads/bilal_key.pem ubuntu@${MASTER_IP} 'chmod +x testing/run_test.sh; chmod +x testing/configs; cd testing; ./run_test.sh'
mkdir -p ../logs/${APP}-${GRAPH_FILE}/sar_logs
scp -i ~/Downloads/bilal_key.pem ubuntu@${MASTER_IP}:~/testing/*.log ../logs/${APP}-${GRAPH_FILE}/
scp -r -i ~/Downloads/bilal_key.pem ubuntu@${MASTER_IP}:~/testing/sar_logs/* ../logs/${APP}-${GRAPH_FILE}/sar_logs/
ssh -i ~/Downloads/bilal_key.pem ubuntu@${MASTER_IP} 'cd testing; rm -rf *.log sar_logs'
