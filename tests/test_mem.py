import sys
import time
from pprint import pprint

from restq import realms 

if __name__ == '__main__':
    jobs = realms.get('test_mem')
    try:
        datasize = int(sys.argv[1])
        jobcount = int(sys.argv[2])
        queuecount = int(sys.argv[3])
        taskcount = int(sys.argv[4])
    except:
        print("test_mem.py <datasize> <jobcount> <queuecount> <taskcount>")
        print("python test_mem.py 20 10000 2 1000")
        exit()
    print("Start insert")
    t = time.time()
    for job_id in range(jobcount):
        for task_id in range(taskcount):
            for queue_id in range(queuecount):
                jobs.add(job_id, task_id, queue_id, "a" * datasize)
    t = time.time() - t
    print("Completed insert in %0.2f" % t)
    pprint(jobs.status)

    print("Start dequeue")
    t = time.time()
    c = 0
    while jobs.pull(5):
        c += 5
    t = time.time() - t
    print("Pulled %s jobs in %0.2f seconds" % (c, t))
    raw_input()
