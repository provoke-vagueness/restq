from __future__ import print_function
import os
import hashlib

import restq




def do_test(url='http://localhost:8080'):
    realms = restq.Realms(url=url)
    test = realms.test
    for i in range(100000):
        job_id = hashlib.md5(str(i)).hexdigest()
        test.add(job_id, 0)

    print(len(test.pull(count=100000)))





if __name__ == '__main__':
    do_test()










