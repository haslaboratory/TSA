import numpy as np

flag = [0] * 100
total_cnt = 0

random_list = np.random.randint(0, 100, 71)

for i in range(71):
    if flag[random_list[i]] == 0:
        flag[random_list[i]] = 1
        total_cnt += 1
        
print(total_cnt)