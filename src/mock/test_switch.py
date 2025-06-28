import numpy as np

HIGHER_LAYER_CNT = 4
LOWER_LAYER_CNT = 4
BUCKET_NUM = int(4096.0 / 800)
print(BUCKET_NUM)
ENABLE_ZNS = True

E_ABSORBED = BUCKET_NUM * 1.0 / (19 * HIGHER_LAYER_CNT / (HIGHER_LAYER_CNT + LOWER_LAYER_CNT))

cnts = np.array([0] * LOWER_LAYER_CNT)
now_cnt = 0

total_cnt = 0
readmit_cnt = 0

flush_cnt = 0
absorbed_cnt = 0

for i in range(100000):
    new_cnt = np.random.binomial(n = 2000000, p = E_ABSORBED / 2000000, size = 1)[0]
    new_nums = np.random.randint(low = 0, high = LOWER_LAYER_CNT, size = new_cnt)
    
    total_cnt += new_cnt
    
    for num in new_nums:
        cnts[int(num)] += 1
        
    now_cnt += new_cnt
    if now_cnt > BUCKET_NUM:
        flush_cnt += 1
        diff = int(now_cnt - BUCKET_NUM)
        
        round_idx = 0
        if ENABLE_ZNS:
            round_idx = i % LOWER_LAYER_CNT
        else:
            for j in range(LOWER_LAYER_CNT):
                if cnts[j] > cnts[round_idx]:
                    round_idx = j
                    
        # print(round_idx)
        
        if cnts[round_idx] >= diff:
            absorbed_cnt += diff
            cnts[round_idx] -= diff
        else:
            diff -= cnts[round_idx]
            absorbed_cnt += cnts[round_idx]
            cnts[round_idx] = 0
            
            sel_idx = round_idx
            while diff >= 1:
                if cnts[sel_idx] > 0:
                    cnts[sel_idx] -= 1
                    diff -= 1
                    conflict = np.random.randint(low = 0, high = 10) < 10
                    if conflict:
                        readmit_cnt += 1
                else:
                    for j in range(LOWER_LAYER_CNT):
                        if j == round_idx:
                            continue
                        if sel_idx == round_idx:
                            sel_idx = j
                            
                        diff_j = (round_idx + LOWER_LAYER_CNT - j) % LOWER_LAYER_CNT
                        diff_sel = (round_idx + LOWER_LAYER_CNT - sel_idx) % LOWER_LAYER_CNT
                            
                        if (cnts[j] / diff_j) > (cnts[sel_idx] / diff_sel):
                            sel_idx = j
                    assert(cnts[sel_idx] != 0)
        now_cnt = BUCKET_NUM
            
            
print(cnts)
print(total_cnt)
print(readmit_cnt)
print(flush_cnt)
print(absorbed_cnt)
print(readmit_cnt / (readmit_cnt + absorbed_cnt))