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

def belong(ch, str):
    return ch in str

def min_freItem(sDB, s, m, minsup):
    s_broadcast = sc.broadcast(s)
    m_broadcast = sc.broadcast(m)
    counter_rdd = sDB.flatMap(lambda line: [(c, 1) for c in line if belong(c, s_broadcast.value) or belong(c, m_broadcast.value)])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)
    fre = counter.filter(lambda x: x[1] >= minsup).keys().collect()
    return sorted(fre)

def gen_candidate(fre_rdd, level):
    def generate_candidates(fre_list):
        candidates = []
        start = 0
        for model in fre_list:
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
                            if start >= len(fre_list):
                                break
                            Q = fre_list[start][0:level - 1]
                        else:
                            break
            except IndexError as e:
                print(f"IndexError: {e} - model: {model}, start: {start}, level: {level}")
                continue
        return sorted(candidates)

    fre_list = fre_rdd.collect()
    fre_broadcast = sc.broadcast(fre_list)
    candidate_rdd = fre_rdd.flatMap(lambda model: generate_candidates([model] + fre_broadcast.value))
    return candidate_rdd.distinct()

if __name__ == '__main__':
    file_path = "hdfs://node1:8020/input/SDB3.txt"
    output_filepath = ""
    strong = "hilkmftwv"
    middle = "rcqgpsyn"
    week = "adeuox"
    min_gap = 0
    max_gap = 3
    min_sup = 500

    sDB = read_file(file_path)
    frequent_items = min_freItem(sDB, strong, middle, min_sup)
    print(frequent_items)

    f_level = 1
    fre_rdd = sc.parallelize(frequent_items)
    candidate_items = gen_candidate(fre_rdd, f_level)
    print("Candidate Items:", candidate_items.collect())
    print(len(candidate_items.collect()))

    sc.stop()