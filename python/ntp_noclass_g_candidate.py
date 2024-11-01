from pyspark import SparkConf, SparkContext
import time

# 初始化SparkContext
conf = SparkConf().setAppName("ntp_noclass_g")
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
    """
    读取HDFS中的文本文件
    :return: 文件RDD
    """
    # 读取HDFS中的文本文件，每行作为一个RDD元素
    lines_rdd = sc.textFile(file_path)
    return lines_rdd


def belong(ch, string):
    """
    判断字符是否在字符串中出现
    :param ch: 字符
    :param string: 字符串
    :return: 是否包含字符
    """
    return ch in string


def min_freItem(sDB, strong_chars, middle_chars, minsup):
    """
    计算初始频繁三支序列模式
    :param sDB: 输入数据库RDD
    :param strong_chars: 强字符集合
    :param middle_chars: 中字符集合
    :param minsup: 最小支持度
    :return: 频繁项列表
    """
    # 使用广播变量传递字符串
    strong_broadcast = sc.broadcast(strong_chars)
    middle_broadcast = sc.broadcast(middle_chars)

    # 使用RDD操作来统计字符频率
    counter_rdd = sDB.flatMap(
        lambda line: [(c, 1) for c in line if belong(c, strong_broadcast.value) or belong(c, middle_broadcast.value)])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)

    # 过滤并收集满足最小支持度的字符
    frequent_items_rdd = counter.filter(lambda x: x[1] >= minsup).keys()
    return frequent_items_rdd


def gen_candidate(fre_rdd, level):
    """
    生成候选模式
    :param fre_rdd: 频繁模式RDD
    :param level: 模式长度
    :return: 候选模式RDD
    """

    # 广播整个fre列表
    fre_broadcast = sc.broadcast(fre_rdd.collect())

    def generate_candidates(partition):
        fre = fre_broadcast.value  # 获取广播的fre数据
        partition = list(partition)  # 将分区数据转换为列表
        candidate = []
        start = 0
        for model in partition:
            R = model[1:level]
            if start < len(fre):
                Q = fre[start][0:level - 1]
            else:
                Q = None
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
        return iter(candidate)

    # 使用 mapPartitions 来并行处理每个分区中的数据
    candidates_rdd = fre_rdd.mapPartitions(generate_candidates)

    # # fre_list = fre_rdd.collect()
    # # 生成频繁项列表并将其作为广播变量分发
    # fre_broadcast = sc.broadcast(fre_rdd.collect())
    #
    # # 用flatMap生成候选模式
    # # candidate_rdd = fre_rdd.flatMap(lambda model: generate_candidates([model] + fre_list))
    # candidate_rdd = fre_rdd.flatMap(lambda model: generate_candidates([model] + fre_broadcast.value))
    # return candidate_rdd.distinct()

    return candidates_rdd


def main():
    # 设置参数
    strong_chars = "hilkmftwv"
    middle_chars = "rcqgpsyn"
    weak_chars = "adeuox"
    minsup = 500
    f_level = 1
    file_path = "hdfs://node1:8020/input/SDB4.txt"

    # 读取文件
    sDB = read_file(file_path)

    # 查找初始频繁三支序列模式
    frequent_items_rdd = min_freItem(sDB, strong_chars, middle_chars, minsup)
    # frequent_items_rdd = min_freItem(sDB, strong_chars, middle_chars, minsup).collect()
    # print("Frequent Items:", frequent_items_rdd)

    # 生成候选模式
    # fre_rdd = sc.parallelize(frequent_items_rdd)
    candidate_items_rdd = gen_candidate(frequent_items_rdd, f_level)

    # 收集并输出候选模式
    candidate_items = sorted(candidate_items_rdd.collect())
    print("Candidate Items:", candidate_items)


if __name__ == '__main__':
    main()

    # 停止SparkContext
    sc.stop()
