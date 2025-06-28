import numpy as np

class Region:
    def __init__(self):
        self.w_ptr_ = 0
        self.f_ptr_ = 0
        self.phy_num_ = 64 * 1000
        self.log_num_ = int(64 * 1000 * 0.95)
        self.slots_ = [-1] * self.phy_num_
        self.indexes_ = [-1] * self.log_num_
        
        self.gc_cnt = 0
        self.mig_cnt = 0
        
    def write(self, key):
        old_idx = self.indexes_[key]
        if old_idx != -1:
            self.slots_[old_idx] = -1
        self.slots_[self.w_ptr_] = key
        self.indexes_[key] = self.w_ptr_
        self.w_ptr_ = (self.w_ptr_ + 1) % self.phy_num_
        
        while self.need_gc():
            self.gc()
        
    def need_gc(self) -> bool:
        valid_cnt = self.f_ptr_ - self.w_ptr_ if self.f_ptr_ > self.w_ptr_ else self.f_ptr_ + self.phy_num_ - self.w_ptr_
        return valid_cnt <= self.phy_num_ * 0.015

    def gc(self):
        self.gc_cnt += 1
        if self.slots_[self.f_ptr_] != -1:
            self.mig_cnt += 1
            key = self.slots_[self.f_ptr_]
            self.indexes_[key] = self.w_ptr_
            self.slots_[self.w_ptr_] = key
            self.w_ptr_ = (self.w_ptr_ + 1) % self.phy_num_
            self.slots_[self.f_ptr_] = -1
        
        self.f_ptr_ = (self.f_ptr_ + 1) % self.phy_num_

class Tiered:
    def __init__(self):
        self.cnt = 8
        self.bucket_cnt = 30
        self.now_cnt = [0] * self.cnt
        self.e_absorbed = self.bucket_cnt * 1.0 / (19 / 5.0)
        self.total_cnt = 0
        
    def calc_now(self) -> int:
        new_cnt = np.random.binomial(n = 2000000, p = self.e_absorbed / 2000000, size = 1)[0]
        new_nums = np.random.uniform(low = 0, high = self.cnt, size = new_cnt)
        
        for num in new_nums:
            self.now_cnt[int(num)] += 1
            
        self.total_cnt += new_cnt
        
        if self.total_cnt <= self.bucket_cnt:
            return -1
        
        diff = self.total_cnt - self.bucket_cnt
            
        round_idx = 0
        for i in range(self.cnt):
            if self.now_cnt[i] > self.now_cnt[round_idx]:
                round_idx = i
                
        if self.now_cnt[round_idx] >= diff:
            self.now_cnt[round_idx] -= diff
        else:
            diff -= self.now_cnt[round_idx]
            self.now_cnt[round_idx] = 0

            sel_idx = round_idx
            while diff > 0:
                if self.now_cnt[sel_idx] > 0:
                    self.now_cnt[sel_idx] -= 1
                    diff -= 1
                else:
                    for j in range(self.cnt):
                        if j == round_idx:
                            continue
                        if sel_idx == round_idx:
                            sel_idx = j
                        diff_j = (round_idx + self.bucket_cnt - j) % self.bucket_cnt
                        diff_sel = (round_idx + self.bucket_cnt - sel_idx) % self.bucket_cnt
                        if (self.now_cnt[j] / diff_j) > (self.now_cnt[sel_idx] / diff_sel):
                            sel_idx = j
        self.total_cnt = self.bucket_cnt
        return round_idx

tiered_cnt = [0] * 8

class ArragedTiered:
    def __init__(self):
        self.cnt = 8
        self.queue = [i for i in range(self.cnt)]

    def calc_now(self) -> int:
        
        idx = 100
        while idx >= self.cnt:
            idx = np.random.zipf(a = 5.5, size = 1)[0] - 1
        tiered_cnt[idx] += 1
            
        round_idx = self.queue[idx]
        self.queue = self.queue[0:idx] + self.queue[idx + 1:] + [round_idx]
        # print(self.queue)
        return round_idx

r0 = Region()

for i in range(400):
    for j in range(int(64 * 1000 * 0.95)):
        cnt = np.random.binomial(n = 20000000, p = 2.0 / 20000000, size = 1)[0]
        if cnt >= 1:
            r0.write(j)
    if i % 40 == 39:
        print("raw", r0.mig_cnt / r0.gc_cnt)

r3 = Region()

# start check
batched_req = np.random.randint(low = 0, high = int(64 * 1000 * 0.95), size = int(64 * 1000 * 0.95))

for i in range(400):
    rand_idx = np.random.randint(low = 0, high = int(64 * 1000 * 0.95), size = 1)[0]
    
    

# r1 = Region()
# arr_tiers = [ArragedTiered() for _ in range(int(8 * 1000 * 0.95))]
# for i in range(3200):
#     for j in range(int(8 * 1000 * 0.95)):
#         tier_idx = arr_tiers[j].calc_now()
#         r1.write(8 * j + tier_idx)
#     if i % 40 == 39:
#         print("arranged", r1.mig_cnt / r1.gc_cnt, tiered_cnt)


# r2 = Region()
# tiers = [Tiered() for _ in range(int(8 * 1000 * 0.95))]

# for i in range(800):
#     for j in range(int(8 * 1000 * 0.95)):
#         tier_idx = tiers[j].calc_now()
#         if tier_idx != -1:
#             r2.write(8 * j + tier_idx)
#     if i % 40 == 39:
#         print("tiered", r2.mig_cnt / r2.gc_cnt)