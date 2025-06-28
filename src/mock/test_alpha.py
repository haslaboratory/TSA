import numpy as np

alpha = 1.0
n_pages = 1000000

slice_num = int(np.sqrt(alpha * n_pages))

acc_cnt = 0
miss_cnt = 0

for i in range(10000000):
    slice_cnt = np.random.binomial(n = n_pages, p = 1 / slice_num, size = 1)[0]
    acc_cnt = slice_cnt * 15
    if slice_cnt > slice_num:
        miss_cnt = slice_cnt + (slice_cnt - slice_num) * 14
    else :
        miss_cnt = slice_cnt
        
print("miss_ratio:", miss_cnt / acc_cnt)
    