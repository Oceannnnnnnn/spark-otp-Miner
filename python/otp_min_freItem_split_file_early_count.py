from pyspark import SparkConf, SparkContext
import hdfs
import os
from collections import Counter, defaultdict

# 初始化 SparkContext
conf = SparkConf().setAppName("otp_min_freItem_split_file_early_count")
sc = SparkContext(conf=conf)


def split_file_independent(input_file_path, num_parts, hdfs_output_dir):
    """
    将 HDFS 上的文件分成多个独立文件存储。
    :param input_file_path: HDFS上的源文件路径
    :param num_parts: 分割文件数量
    :param hdfs_output_dir: HDFS目标目录
    """
    # 初始化 HDFS 客户端
    client = hdfs.InsecureClient("http://node1:9870", user='your_username')  # 请根据实际用户名替换

    # 检查并清理输出目录
    if client.status(hdfs_output_dir, strict=False):
        client.delete(hdfs_output_dir, recursive=True)
    client.makedirs(hdfs_output_dir)

    # 读取 HDFS 文件内容
    with client.read(input_file_path, encoding='utf-8') as reader:
        lines = reader.readlines()

    # 计算每个分割文件的行数
    lines_per_part = len(lines) // num_parts

    # 将数据分割并存储到 HDFS
    for i in range(num_parts):
        # 最后一个分区包含所有剩余的行
        if i == num_parts - 1:
            part_lines = lines[i * lines_per_part:]
        else:
            part_lines = lines[i * lines_per_part: (i + 1) * lines_per_part]

        # 输出文件路径
        output_file_path = os.path.join(hdfs_output_dir, f"output_part_{i + 1}.txt")

        # 将分割部分写入 HDFS，逐行写入避免 AsyncWriter 的限制
        with client.write(output_file_path, encoding='utf-8') as writer:
            for line in part_lines:
                writer.write(line)
        print(f"Saved split file to {output_file_path}")


def min_freItem(sDB_partition, strong_broadcast, middle_broadcast, minsup):
    """
    计算初始频繁一长度序列模式。
    一旦模式在当前分区达到 minsup，就立即将其添加为频繁项。
    """
    counter = defaultdict(int)
    frequent_items = []

    for _, line in sDB_partition:
        for c in line:
            # 检查字符是否在强字符或中字符集合中
            if c in strong_broadcast.value or c in middle_broadcast.value:
                counter[c] += 1
                if counter[c] >= minsup:
                    # 一旦计数达到 minsup，立即添加到频繁项列表中
                    frequent_items.append((c, counter[c]))
                    del counter[c]  # 从计数器中移除以避免继续计数

    # 添加剩余的满足最小支持度的项
    frequent_items.extend((item, count) for item, count in counter.items() if count >= minsup)
    return frequent_items

# def min_freItem(sDB_partition, strong_broadcast, middle_broadcast, minsup):
#     """
#         计算初始频繁三支序列模式
#         :param sDB: 输入数据库RDD
#         :param strong_chars: 强字符集合
#         :param middle_chars: 中字符集合
#         :param minsup: 最小支持度
#         :return: 频繁项RDD
#     """
#
#     counter = Counter()
#     for _, line in sDB_partition:
#         # 将强字符和中字符筛选后统计频次
#         counter.update([c for c in line if c in strong_broadcast.value or c in middle_broadcast.value])
#
#     # 返回频次大于等于 minsup 的字符及其频率
#     return ((item, count) for item, count in counter.items() if count >= minsup)

    # # 使用RDD操作来统计字符频率
    # counter = {}
    # for line in sDB_partition:
    #     for c in line[1]:  # line[1] contains the file contents in wholeTextFiles
    #         if c in strong_broadcast.value or c in middle_broadcast.value:
    #             counter[c] = counter.get(c, 0) + 1
    #
    # # 返回满足最小支持度的字符
    # return [(k, v) for k, v in counter.items() if v >= minsup]


def main():
    # 设置参数
    strong_chars = "hilkmftwv"
    middle_chars = "rcqgpsyn"
    weak_chars = "adeuox"
    minsup = 80000
    f_level = 1

    #广播变量传递字符串
    strong_broadcast = sc.broadcast(strong_chars)
    middle_broadcast = sc.broadcast(middle_chars)

    # HDFS 文件路径
    hdfs_file_path = "/input/SDB4_100.txt"
    # HDFS 输出路径
    hdfs_output_dir = "/input/split"
    # 将文件分为 3 份
    split_file_independent(hdfs_file_path, 3, hdfs_output_dir)

    # 读取分割后的文件并输出统计
    sDB = sc.wholeTextFiles(f"hdfs://node1:8020{hdfs_output_dir}")

    # 使用 mapPartitions 调用 min_freItem 计算每个分区的频繁项
    frequent_items_rdd = sDB.mapPartitions(lambda partition: min_freItem(partition, strong_broadcast, middle_broadcast, minsup))
    # 汇总频繁项的支持度
    frequent_items_total = frequent_items_rdd.reduceByKey(lambda a, b: a + b).collect()

    # 输出结果
    for item, count in frequent_items_total:
        print(f"Item: {item}, Support: {count}")


if __name__ == '__main__':
    main()

    # 停止 SparkContext
    sc.stop()
