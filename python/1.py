from pyspark import SparkConf, SparkContext
import time

# 初始化 SparkContext
conf = SparkConf().setAppName("ntp_miner_spark")
sc = SparkContext(conf=conf)

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

def read_file(file_path):
    return sc.textFile(file_path)

def belong(ch, string):
    return ch in string

def min_freItem(sDB, strong_chars, middle_chars, minsup):
    strong_broadcast = sc.broadcast(strong_chars)
    middle_broadcast = sc.broadcast(middle_chars)
    counter_rdd = sDB.flatMap(
        lambda line: [(c, 1) for c in line if belong(c, strong_broadcast.value) or belong(c, middle_broadcast.value)])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)
    fre = counter.filter(lambda x: x[1] >= minsup).keys().collect()
    return sorted(fre)

def gen_candidate(fre_list, level):
    def generate_candidates(partition):
        candidates = []
        start = 0
        for model in partition:
            try:
                R = model[1:level]
                Q = fre_list[start][0:level - 1]
                if Q != R:
                    start = binary_search(fre_list, R, level)
                if start < 0 or start >= len(fre_list):
                    start = 0
                else:
                    Q = fre_list[start][0:level - 1]
                    while Q == R:
                        if start < len(fre_list):
                            candidates.append(model[0:level] + fre_list[start][level - 1:level])
                            start += 1
                        else:
                            break
                        if start < len(fre_list):
                            Q = fre_list[start][0:level - 1]
            except IndexError as e:
                print(f"IndexError: {e} - model: {model}, start: {start}, level: {level}")
                continue
        return iter(candidates)

    return sc.parallelize(fre_list).mapPartitions(generate_candidates).distinct()

def compute_support(p, sDB, minsup):
    sDB_list = sDB.collect()
    sDB_broadcast = sc.broadcast(sDB_list)

    def compute_occurrences(seq):
        nettree = []
        return create_nettree(nettree, seq)

    occnum = sc.parallelize(sDB_broadcast.value).map(compute_occurrences).reduce(lambda a, b: a + b)
    if occnum >= minsup:
        return [p]
    else:
        return []

def create_nettree(nettree, seq, sub_ptn, s, m, w):
    occurnum = 0
    for i in range(0, len(sub_ptn) + 1):
        nettree.append([])
    for i in range(0, len(seq) - len(sub_ptn)):
        if seq[i] != sub_ptn[0].start:
            continue
        nettree[0].append(i)
        occurnum += create_subnettree(nettree, seq, i, 2, sub_ptn, s, m, w)
    return occurnum

def create_subnettree(nettree, seq, parent, L, sub_ptn, s, m, w):
    if L > len(sub_ptn) + 1:
        return 1
    for i in range(parent + 1, parent + sub_ptn[L - 2].min + 1):
        if belong(seq[i], s):
            return 0
    for i in range(parent + sub_ptn[L - 2].min + 1, parent + sub_ptn[L - 2].max + 2):
        if i >= len(seq):
            break
        if seq[i] == sub_ptn[L - 2].end:
            k = len(nettree[L - 1])
            flag = -1
            for j in range(k):
                if i == nettree[L - 1][j]:
                    flag = j
                    break
            if flag == -1:
                nettree[L - 1].append(i)
                if create_subnettree(nettree, seq, i, L + 1, sub_ptn, s, m, w):
                    return 1
        if not belong(seq[i], m) and not belong(seq[i], w):
            break
    return 0

def main():
    strong_chars = "hilkmftwv"
    middle_chars = "rcqgpsyn"
    weak_chars = "adeuox"
    minsup = 500
    f_level = 1
    file_path = "hdfs://node1:8020/input/SDB4.txt"

    sDB = read_file(file_path)
    sub_ptn = []

    frequent_items = min_freItem(sDB, strong_chars, middle_chars, minsup)
    candidates_rdd = gen_candidate(frequent_items, f_level)
    candidate_items = sorted(candidates_rdd.collect())
    print("Candidate Items:", candidate_items)

    freArr = []
    while True:
        next_fre_rdd = candidates_rdd.flatMap(lambda p: compute_support(p, sDB, minsup))
        if len(next_fre_rdd.collect()) == 0:
            break
        freArr.append(next_fre_rdd.collect())
        f_level += 1
        candidates_rdd = gen_candidate(next_fre_rdd, f_level)

    print(freArr)

if __name__ == '__main__':
    main()
    sc.stop()