import matplotlib.pyplot as plt
import numpy as np

E_ABSORBED = 4096 / 19 / 396

log_readmit_cnt = 0
set_flush_cnt = 0
absorbed_cnt = 0

total_cnt = 0
this_cnt = 0

for i in range(200000):
    this_cnt = np.random.binomial(n = 20000000, p = E_ABSORBED / 20000000, size = 1)[0]
    print(this_cnt)
    if this_cnt < 2:
        log_readmit_cnt += this_cnt
        continue
    
    set_flush_cnt += 1
    absorbed_cnt += this_cnt
    this_cnt = 0

print(absorbed_cnt / set_flush_cnt)
print(log_readmit_cnt / absorbed_cnt)
print(set_flush_cnt * 10.0 / absorbed_cnt)