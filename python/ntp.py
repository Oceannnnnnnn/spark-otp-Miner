from pyspark import SparkConf, SparkContext

# 初始化SparkContext
conf = SparkConf().setAppName("ntp_miner_spark")
sc = SparkContext(conf=conf)


def binary_search(fre, cand, level):
    """
    :param fre: 频繁模式列表
    :param cand: 目标模式
    :param level: 频繁模式长度
    :return: 目标模式在频繁模式列表位置
    """
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


def read_file():
    file_path = "hdfs://node1:8020/input/SDB3.txt"
    # 读取HDFS中的文本文件，每行作为一个RDD元素
    lines_rdd = sc.textFile(file_path)
    return lines_rdd


def belong(ch, str):
    """
    :param ch: 字符
    :param str: 字符串
    :return: 判断字符是否在字符串中出现，返回bool值
    """
    return ch in str


def min_freItem(sDB, s, m, minsup):
    """
    :return: 初始频繁三支序列模式
    """
    # 使用广播变量传递字符串
    s_broadcast = sc.broadcast(s)
    m_broadcast = sc.broadcast(m)

    # 使用RDD操作来统计字符频率
    counter_rdd = sDB.flatMap(lambda line: [(c, 1) for c in line if belong(c, s_broadcast.value) or belong(c, m_broadcast.value)])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)

    # 过滤并收集满足最小支持度的字符
    fre = counter.filter(lambda x: x[1] >= minsup).keys().collect()
    return sorted(fre)


def gen_candidate(fre_rdd, level):
    """
    :param fre_rdd: 频繁模式RDD
    :param level: 频繁模式长度
    :return: 候选模式RDD
    """
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
        # print(len(candidate), level, len(fre))
        return sorted(candidates)

    #生成频繁项列表并将其作为广播变量分发
    fre_list = fre_rdd.collect()
    fre_broadcast = sc.broadcast(fre_list)

    # 用flatMap生成候选模式
    candidate_rdd = fre_rdd.flatMap(lambda model: generate_candidates([model] + fre_broadcast.value))
    return candidate_rdd.distinct()


class NTP_Miner:
    """
    NTP_Miner算法
    """
    output_filepath = ""
    output_filename = "OTP-Miner-Output.txt"
    mingap, maxgap = 0, 3
    minsup = 500
    s = "hilkmftwv"
    m = "rcqgpsyn"
    w = "adeuox"
    sDB = None
    sub_ptn = []

    def __init__(self, file_path='', output_filepath='',
                 strong="hilkmftwv", middle="rcqgpsyn", week="adeuox",
                 min_gap=0, max_gap=3, min_sup=500):
        """
        :param file_path: 输入文件位置
        :param output_filepath: 输出文件夹位置
        :param strong: 强字符串
        :param middle: 中字符串
        :param week: 弱字符串
        :param min_gap: 最小间隙
        :param max_gap: 最大间隙
        :param min_sup: 最小支持度
        """
        self.sDB = read_file()  # RDD
        self.output_filepath = output_filepath
        self.mingap, self.maxgap = min_gap, max_gap
        self.minsup = min_sup
        self.s = strong
        self.m = middle
        self.w = week

    def find_min_freItem(self):
        """
        :return: 初始频繁三支序列模式
        """
        return min_freItem(self.sDB, self.s, self.m, self.minsup)

    def generate_candidates(self, frequent_items, level):
        """
        :param frequent_items: 初始频繁三支序列模式
        :param level: 模式长度
        :return: 候选模式集合
        """
        fre_rdd = sc.parallelize(frequent_items)
        return gen_candidate(fre_rdd, level)


if __name__ == '__main__':
    compnum = 0
    f_level = 1
    miner = NTP_Miner()
    frequent_items = miner.find_min_freItem()
    print(frequent_items)

    candidate_items = miner.generate_candidates(frequent_items, f_level)
    print("Candidate Items:", candidate_items.collect())
    print(len(candidate_items.collect()))

# 停止SparkContext
sc.stop()
