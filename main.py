import json
import os
import time
import tqdm

QUERY_FILE_PATH = "./query.json"
TSFILE_PATH = "/data/iotdb/sequence/root.test/0/1.tsfile"
PHYSICAL_ORDER_PATH = "./physical_order.txt"
LOG_FILE_PATH = "./benchmark.log"


def main():
    with open(QUERY_FILE_PATH, 'r') as f:
        query_info = json.load(f)
    if os.path.exists(LOG_FILE_PATH):
        os.remove(LOG_FILE_PATH)
    with open(LOG_FILE_PATH, 'w') as f:
        pass
    for query in tqdm.tqdm(query_info):
        os.system("echo 3 |sudo tee /proc/sys/vm/drop_caches")
        start_time = query["startTime"]
        end_time = query["endTime"]
        sensors = query["sensors"]
        os.system("mvn exec:java -Dexec.mainClass=SingleReplicaBenchmark %s %s %s %d %d %s" % (
            TSFILE_PATH, LOG_FILE_PATH, PHYSICAL_ORDER_PATH, start_time, end_time, " ".join(sensors)
        ))
        time.sleep(0.5)

if __name__ == '__main__':
    main()
