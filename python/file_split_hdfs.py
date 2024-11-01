from pyspark import SparkConf, SparkContext
import hdfs
import os

# 初始化 SparkContext
conf = SparkConf().setAppName("file_split_independent_files")
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

def min_freItem(sDB, strong_chars, middle_chars, minsup):
    """
        计算初始频繁三支序列模式
        :param sDB: 输入数据库RDD
        :param strong_chars: 强字符集合
        :param middle_chars: 中字符集合
        :param minsup: 最小支持度
        :return: 频繁项RDD
    """
    #广播变量传递字符串
    strong_chars_broadcast = sc.broadcast(strong_chars)
    middle_chars_broadcast = sc.broadcast(middle_chars)

    # 使用RDD操作来统计字符频率
    counter_rdd = sDB.flatMap(
        lambda line: [(c, 1) for c in line[1] if c in strong_chars_broadcast.value or c in middle_chars_broadcast.value])
    counter = counter_rdd.reduceByKey(lambda a, b: a + b)

    # 过滤并返回满足最小支持度的字符
    return counter.filter(lambda x: x[1] >= minsup)


def main():
    # 设置参数
    strong_chars = "hilkmftwv"
    middle_chars = "rcqgpsyn"
    weak_chars = "adeuox"
    minsup = 500
    f_level = 1

    # HDFS 文件路径
    hdfs_file_path = "/input/SDB4.txt"
    # HDFS 输出路径
    hdfs_output_dir = "/input/split"
    # 将文件分为 3 份
    split_file_independent(hdfs_file_path, 3, hdfs_output_dir)

    # 读取分割后的文件并输出统计
    sDB = sc.wholeTextFiles(f"hdfs://node1:8020{hdfs_output_dir}")

    # 使用 min_freItem 计算频繁项，并汇总支持度
    frequent_items_rdd = min_freItem(sDB, strong_chars, middle_chars, minsup)
    frequent_items_total = frequent_items_rdd.collect()

    # 输出结果
    for item, count in frequent_items_total:
        print(f"Item: {item}, Support: {count}")


if __name__ == '__main__':
    main()

    # 停止 SparkContext
    sc.stop()
