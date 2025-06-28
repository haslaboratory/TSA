import numpy as np

HIGHER_LAYER_CNT = 19
LOWER_LAYER_CNT = 1

BUCKET_NUM = 4096.0 / 245

E_ABSORBED = BUCKET_NUM / (19 * HIGHER_LAYER_CNT / (LOWER_LAYER_CNT + HIGHER_LAYER_CNT))

ENABLE_ZNS = True

high_flush_cnt = 0
high_absorbed_cnt = 0
low_flush_cnt = 0
low_absorbed_cnt = 0

for i in range(10000):
    tot_cnt = 0
    for j in range(HIGHER_LAYER_CNT):
        this_cnt = np.random.binomial(n = 20000000, p = E_ABSORBED / 20000000, size = 1)[0]
        tot_cnt += this_cnt
        if ENABLE_ZNS:
            high_flush_cnt += 1
            high_absorbed_cnt += this_cnt
        elif this_cnt > 0:
            high_flush_cnt += 1
            high_absorbed_cnt += this_cnt
    
    new_nums = np.random.uniform(low = 0, high = LOWER_LAYER_CNT, size = tot_cnt)

    cnts = np.array([0] * LOWER_LAYER_CNT)
    for num in new_nums:
        cnts[int(num)] += 1
    
    for j in range(LOWER_LAYER_CNT):
        if ENABLE_ZNS:
            low_flush_cnt += 1
            low_absorbed_cnt += cnts[j]
        elif cnts[j] > 0:
            low_flush_cnt += 1
            low_absorbed_cnt += cnts[j]
            
print(high_flush_cnt)
print(high_absorbed_cnt)
print(low_flush_cnt)
print(low_absorbed_cnt)
print(high_absorbed_cnt / high_flush_cnt)
print(low_absorbed_cnt / low_flush_cnt)