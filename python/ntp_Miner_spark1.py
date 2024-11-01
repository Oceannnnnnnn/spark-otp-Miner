from pyspark import SparkConf, SparkContext
import time

def binary_search(fre, cand, level):
    low, high = 0, len(fre) - 1
    if low > high:
        return -1
    while low < high:
        mid = int((low + high) / 2)
        if cand <= fre[mid][0:level - 1]:
            high = mid
        else:
            low = mid + 1
    if cand == fre[low][0:level - 1]:
        return low
    elif low + 1 < len(fre) and cand == fre[low + 1][0:level - 1]:
        return low + 1
    else:
        return -1

def gen_candidate(fre, level):
    candidate = []
    start = 0
    for model in fre:
        R = model[1:level]
        Q = fre[start][0:level - 1]
        if Q != R:
            start = binary_search(fre, R, level)
        if start < 0 or start >= len(fre):
            start = 0
        else:
            Q = fre[start][0:level - 1]
            while Q == R:
                candidate.append(model[0:level] + fre[start][level - 1:level])
                start = start + 1
                if start >= len(fre):
                    start = 0
                    break
                Q = fre[start][0:level - 1]
    return candidate

class NTP_Miner:
    output_filepath = ""
    output_filename = "OTP-Miner-Output.txt"
    mingap, maxgap = 0, 3
    minsup = 500
    s = "hilkmftwv"
    m = "rcqgpsyn"
    w = "adeuox"
    sDB = []
    sub_ptn = []

    def __init__(self, file_path='', output_filepath='', strong="hilkmftwv", middle="rcqgpsyn", week="adeuox", min_gap=0, max_gap=3, min_sup=500):
        self.sDB = sc.textFile(file_path).collect()
        self.output_filepath = output_filepath
        self.mingap, self.maxgap = min_gap, max_gap
        self.minsup = min_sup
        self.s = strong
        self.m = middle
        self.w = week

    class sub_ptn_struct:
        start = ''
        end = ''
        min, max = 0, 0

        def __init__(self, start, end, min, max):
            self.start = start
            self.end = end
            self.min = min
            self.max = max

    def min_freItem(self):
        counter = {}
        fre = []
        for strs in self.sDB:
            for c in strs:
                if c in self.s or c in self.m:
                    if c in counter:
                        counter[c] += 1
                    else:
                        counter[c] = 1
        for key in counter.keys():
            if counter[key] >= self.minsup:
                fre.append(key)
        return sorted(fre)

    def deal_range(self, pattern):
        sub_ptn = []
        if len(pattern) == 1:
            sub_ptn.append(self.sub_ptn_struct(pattern[0], "", 0, 0))
        for i in range(0, len(pattern) - 1):
            sub_ptn.append(self.sub_ptn_struct(pattern[i], pattern[i + 1], self.mingap, self.maxgap))
        return sub_ptn

    def create_nettree(self, nettree, seq):
        occurnum = 0
        for i in range(0, len(self.sub_ptn) + 1):
            nettree.append([])
        for i in range(0, len(seq) - len(self.sub_ptn)):
            if seq[i] != self.sub_ptn[0].start:
                continue
            nettree[0].append(i)
            occurnum = occurnum + self.create_subnettree(nettree, seq, i, 2)
        return occurnum

    def create_subnettree(self, nettree, seq, parent, L):
        if L > len(self.sub_ptn) + 1:
            return 1
        for i in range(parent + 1, parent + self.sub_ptn[L - 2].min + 1):
            if self.belong(seq[i], self.s):
                return 0
        for i in range(parent + self.sub_ptn[L - 2].min + 1, parent + self.sub_ptn[L - 2].max + 2):
            if i >= len(seq):
                break
            if seq[i] == self.sub_ptn[L - 2].end:
                k = len(nettree[L - 1])
                flag = -1
                for j in range(k):
                    if i == nettree[L - 1][j]:
                        flag = j
                        break
                if flag == -1:
                    nettree[L - 1].append(i)
                    if self.create_subnettree(nettree, seq, i, L + 1):
                        return 1
            if not self.belong(seq[i], self.m) and not self.belong(seq[i], self.w):
                break
        return 0

    def belong(self, ch, str):
        return ch in str

    def output(self, freArr):
        output_file = open(self.output_filepath + "/" + self.output_filename, 'w')
        for fre in freArr:
            strArr = ""
            for strs in fre:
                strArr += strs + " "
            output_file.write(strArr + "\n")

    def solve_test(self):
        compnum = 0
        f_level = 1
        freArr = []
        begin_time = time.time()
        print("算法开始运行，请稍等。")
        fre = self.min_freItem()
        freArr.append(fre)
        candidate = gen_candidate(fre, f_level)
        while len(candidate) != 0:
            next_fre = []
            for p in candidate:
                occnum = 0
                compnum = compnum + 1
                for strs in self.sDB:
                    if len(strs) > 0:
                        self.sub_ptn = self.deal_range(p)
                        num = 0
                        if len(self.sub_ptn) + 1 > len(strs):
                            num = 0
                        else:
                            nettree = []
                            num = self.create_nettree(nettree, strs)
                        occnum += num
                    if occnum >= self.minsup:
                        next_fre.append(p)
                        break
            f_level += 1
            freArr.append(next_fre)
            candidate = gen_candidate(next_fre, f_level)
        end_time = time.time()
        time_consuming = end_time - begin_time
        freNum = 0
        print("挖掘结果为：")
        for fre in freArr:
            strArr = ""
            for strs in fre:
                strArr += strs + " "
                freNum += 1
            print(strArr)
        print("The number of frequent patterns:", freNum)
        print("The time-consuming:", time_consuming * 1000, "ms")
        print("The number of calculation:", compnum)
        return freArr

if __name__ == '__main__':
    conf = SparkConf().setAppName("NTP_Miner").setMaster("yarn")
    sc = SparkContext(conf=conf)
    file = "hdfs://node1:8020/input/SDB3.txt"
    s = "hilkmftwv"
    m = "rcqgpsyn"
    w = "adeuox"
    min_gap = 0
    max_gap = 3
    min_sup = 400
    NTP_Miner(file_path=file, strong=s, middle=m, week=w, min_gap=min_gap, max_gap=max_gap, min_sup=min_sup).solve_test()
    sc.stop()