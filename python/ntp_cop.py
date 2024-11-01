from pyspark import SparkConf, SparkContext
import time

# 初始化 SparkContext
conf = SparkConf().setAppName("NTP_Miner_Spark_cop")
sc = SparkContext(conf=conf)

def read_file(file_path):
    return sc.textFile(file_path)

def belong(ch, str):
    return ch in str

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


def gen_candidate(fre_rdd, level):
    def generate_candidates(fre_list):
        candidates = []
        start = 0
        for model in fre_list:
            R = model[1:level]
            Q = fre_list[start][0:level - 1]
            if Q != R:
                start = binary_search(fre_list, R, level)
            if start < 0 or start >= len(fre_list):
                start = 0
            else:
                Q = fre_list[start][0:level - 1]
                while Q == R:
                    candidates.append(model[0:level] + fre_list[start][level - 1:level])
                    start += 1
                    if start >= len(fre_list):
                        start = 0
                        break
                    Q = fre_list[start][0:level - 1]
        return candidates

    fre_list = fre_rdd.collect()
    fre_broadcast = sc.broadcast(fre_list)

    # 使用广播变量而非直接传递 fre_list
    candidate_rdd = fre_rdd.flatMap(lambda model: generate_candidates([model] + fre_broadcast.value))
    return candidate_rdd.distinct()


class NTP_Miner:
    output_filepath = ""
    output_filename = "OTP-Miner-Output.txt"
    mingap, maxgap = 0, 3
    minsup = 500
    s = "hilkmftwv"
    m = "rcqgpsyn"
    w = "adeuox"
    sDB = None
    sub_ptn = []

    def __init__(self, file_path='', output_filepath='', strong="hilkmftwv", middle="rcqgpsyn", week="adeuox", min_gap=0, max_gap=3, min_sup=500):
        self.sDB = read_file(file_path)
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
        s_broadcast = sc.broadcast(self.s)
        m_broadcast = sc.broadcast(self.m)

        # 避免在闭包中使用 SparkContext 或其相关对象
        def map_function(line):
            return [(c, 1) for c in line if belong(c, s_broadcast.value) or belong(c, m_broadcast.value)]

        counter_rdd = self.sDB.flatMap(map_function)
        counter = counter_rdd.reduceByKey(lambda a, b: a + b)

        # 在闭包中使用广播变量的值，而非 SparkContext
        frequent_items = counter.filter(lambda x: x[1] >= self.minsup).keys().collect()

        # 释放广播变量，避免占用内存
        s_broadcast.unpersist()
        m_broadcast.unpersist()

        return sorted(frequent_items)

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

    def solve(self):
        print("NTP_Miner 算法开始运行，请等待...")
        print("算法参数：")
        print("强字符集：" + self.s + " 中字符集：" + self.m + " 弱字符集：" + self.w)
        print("mingap:" + str(self.mingap) + " maxgap:" + str(self.maxgap) + " minsup:" + str(self.minsup))
        compnum = 0
        f_level = 1
        freArr = []

        begin_time = time.time()
        fre = self.min_freItem()
        freArr.append(fre)
        candidate_rdd = sc.parallelize(fre)
        while not candidate_rdd.isEmpty():
            next_fre = []
            candidate_list = candidate_rdd.collect()
            for p in candidate_list:
                occnum = 0
                compnum = compnum + 1
                for strs in self.sDB.collect():
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
            candidate_rdd = sc.parallelize(next_fre)
            candidate_rdd = gen_candidate(candidate_rdd, f_level)

        end_time = time.time()
        time_consuming = end_time - begin_time
        freNum = 0
        print("NTP_Miner 算法运行完毕。")
        for fre in freArr:
            strArr = ""
            for strs in fre:
                strArr += strs + " "
                freNum += 1
            print(strArr)
        print("The number of frequent patterns:", freNum)
        print("The time-consuming:", time_consuming * 1000, "ms")
        print("The number of calculation:", compnum)
        self.output(freArr)
        print("挖掘结果已写入：" + self.output_filepath + "/" + self.output_filename)

if __name__ == '__main__':
    file = "hdfs://node1:8020/input/SDB3.txt"
    s = "hilkmftwv"
    m = "rcqgpsyn"
    w = "adeuox"
    min_gap = 0
    max_gap = 3
    min_sup = 400
    NTP_Miner(file_path=file, strong=s, middle=m, week=w, min_gap=min_gap, max_gap=max_gap, min_sup=min_sup).solve()

# 停止 SparkContext
sc.stop()