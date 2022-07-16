from celery import Celery
from collections import Counter
from celery.exceptions import SoftTimeLimitExceeded
app = Celery('2021MCS3169', broker='pyamqp://guest@localhost//',backend='redis://localhost:6579', worker_prefetch_multiplier=1)

@app.task(acks_late=True, soft_time_limit=60, default_retry_delay=0, time_limit=60, autoretry_for=(SoftTimeLimitExceeded,))
def shar(namfil1):
    wa = {}
    for namfil in namfil1:
        with open(namfil, mode='r', newline='\r') as o:
            for q in o:
                if q == '\n':
                    continue
                sp = q.split(',')[4:-2]
                t = " ".join(sp)
                for tof in t.split(" "):
                    if tof not in wa:
                        wa[tof] = 0
                    wa[tof] = wa[tof] + 1
    return wa

@app.task(acks_late=True, soft_time_limit=60, default_retry_delay=0, time_limit=60, autoretry_for=(SoftTimeLimitExceeded,))
def callb(wcd):
    finala = {}
    for wc in wcd:
        finala = Counter(wc) + Counter(finala)
    return finala

