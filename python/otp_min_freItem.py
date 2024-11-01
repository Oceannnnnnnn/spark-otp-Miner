from pyspark import SparkConf, SparkContext
import time

# 初始化SparkContext
conf = SparkConf().setAppName("otp_min_freItem")
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


def read_file(file_path, num_partitions):
    """
    读取HDFS中的文本文件
    :return: 文件RDD
    """
    # 读取HDFS中的文本文件，每行作为一个RDD元素
    lines_rdd = sc.textFile(file_path, num_partitions)
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
        lambda line: [(c, 1) for c in line if c in strong_broadcast.value or c in middle_broadcast.value])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)

    # 过滤并收集满足最小支持度的字符
    frequent_items_rdd = counter.filter(lambda x: x[1] >= minsup).keys()
    # frequent_items = counter.filter(lambda x: x[1] >= minsup).keys().collect()
    return frequent_items_rdd


def gen_candidate(fre_rdd, level):
    """
    生成候选模式，直接在RDD中局部生成，无需广播。
    :param fre_rdd: 频繁模式RDD
    :param level: 模式长度
    :return: 候选模式RDD
    """

    def generate_candidates_partition(partition):
        """
        在每个RDD分区内部生成候选项，避免大量数据回传给driver。
        """
        fre_list = sorted(list(partition))  # 将分区中的数据收集到本地并排序
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
        return sorted(candidates)

    # 在每个分区中局部生成候选模式，而不是收集到driver
    candidate_rdd = fre_rdd.mapPartitions(generate_candidates_partition)

    return candidate_rdd.distinct()

# def gen_candidate_with_join(fre_rdd, level):
#     """
#     使用join生成候选模式
#     :param fre_rdd: 频繁模式RDD
#     :param level: 模式长度
#     :return: 候选模式RDD
#     """
#
#     # 创建两个RDD，分别提取前缀和后缀
#     prefix_rdd = fre_rdd.map(lambda x: (x[0:level - 1], x))  # 生成前缀部分
#     suffix_rdd = fre_rdd.map(lambda x: (x[1:level], x))  # 生成后缀部分
#
#     # Join前缀和后缀，合并候选模式
#     candidate_rdd = prefix_rdd.join(suffix_rdd) \
#         .map(lambda x: x[1][0] + x[1][1][-1]) \
#         .distinct()
#
#     return candidate_rdd


# def gen_candidate(fre_rdd, level):
#     """
#     使用Spark的内置转换生成候选模式
#     :param fre_rdd: 频繁模式RDD
#     :param level: 模式长度
#     :return: 候选模式RDD
#     """
#
#     def generate_candidate_pairs(item1, item2):
#         """
#         根据两个频繁模式生成候选模式
#         """
#         if item1[1:level] == item2[0:level - 1]:
#             return [item1 + item2[level - 1]]
#         else:
#             return []
#
#     # 将fre_rdd与自身进行笛卡尔积，尝试组合模式生成候选模式
#     candidate_rdd = fre_rdd.cartesian(fre_rdd) \
#         .flatMap(lambda pair: generate_candidate_pairs(pair[0], pair[1])) \
#         .distinct()
#
#     return candidate_rdd



def main():
    # 设置参数
    strong_chars = "hilkmftwv"
    middle_chars = "rcqgpsyn"
    weak_chars = "adeuox"
    minsup = 500
    f_level = 1
    file_path = "hdfs://node1:8020/input/SDB4_100.txt"

    # 读取文件
    sDB = read_file(file_path, 3)

    # 查找初始频繁三支序列模式
    frequent_items_rdd = min_freItem(sDB, strong_chars, middle_chars, minsup)
    print("Frequent Items:", frequent_items_rdd.collect())

    # 生成候选模式
    # fre_rdd = sc.parallelize(frequent_items_rdd)
    # candidate_items_rdd = gen_candidate(frequent_items_rdd, f_level)

    # 收集并输出候选模式
    # candidate_items = candidate_items_rdd.collect()
    # print("Candidate Items:", candidate_items)


if __name__ == '__main__':
    main()

    # 停止SparkContext
    sc.stop()
