import sys
import time
from pprint import pprint

import restq


if __name__ == '__main__':
    try:
        datasize = int(sys.argv[1])
        jobcount = int(sys.argv[2])
        queuecount = int(sys.argv[3])
        taskcount = int(sys.argv[4])
    except:
        print("test_mem.py <datasize> <jobcount> <queuecount> <taskcount>")
        print("python test_mem.py 20 10000 2 1000"
        exit()
    t = time.time()
    for job_id in range(jobcount):
        for task_id in range(taskcount):
            for queue_id in range(queuecount):
                restq.jobs.add(job_id, task_id, queue_id, "a" * datasize)
    t = time.time() - t
    print("Completed insert in %0.2f" % t)
    pprint(restq.jobs.status)
    raw_input()
