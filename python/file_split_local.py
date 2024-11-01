from pyspark import SparkConf, SparkContext
from hdfs import InsecureClient  # 引入hdfs库
import os

# 初始化SparkContext
conf = SparkConf().setAppName("file_split_local")
sc = SparkContext(conf=conf)

def read_file(file_path):
    """
    读取HDFS中的文本文件
    :return: 文件RDD
    """
    # 读取HDFS中的文本文件，每行作为一个RDD元素
    lines_rdd = sc.textFile(file_path)
    return lines_rdd

def split_file(input_file, n, hdfs_output_dir, hdfs_client):
    """
    将文件分割成多个小文件并上传到HDFS
    :param input_file: 本地文件路径
    :param n: 分割文件数量
    :param hdfs_output_dir: HDFS 目标目录
    :param hdfs_client: HDFS 客户端
    """
    # 读取输入文件中的所有行
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # 创建临时文件来存储分割数据
    local_temp_dir = '/tmp/split_files'
    os.makedirs(local_temp_dir, exist_ok=True)

    output_files = [os.path.join(local_temp_dir, f'output_{i+1}.txt') for i in range(n)]
    file_handlers = [open(output_file, 'w') for output_file in output_files]
    file_lengths = [0] * n

    # 将输入文件中的每一行分配到最短的输出文件中
    for line in lines:
        line = line.strip()
        min_length_index = file_lengths.index(min(file_lengths))
        file_handlers[min_length_index].write(line + '\n')
        file_lengths[min_length_index] += len(line.split())

    # 关闭所有临时文件
    for handler in file_handlers:
        handler.close()

    # 将临时文件上传到HDFS
    for local_file in output_files:
        hdfs_file_path = os.path.join(hdfs_output_dir, os.path.basename(local_file))
        with open(local_file, 'rb') as f:
            hdfs_client.write(hdfs_file_path, f, overwrite=True)
        print(f"Uploaded {local_file} to {hdfs_file_path}")

    # 删除本地临时文件
    for local_file in output_files:
        os.remove(local_file)

    os.rmdir(local_temp_dir)

def main():
    # 本地文件路径
    file_path = "/tmp/pycharm_project_704/datasets/SDB4.txt"

    # HDFS 目标路径
    hdfs_output_dir = "/input/split"

    # 初始化 HDFS 客户端（实际的 HDFS 地址）
    hdfs_client = InsecureClient("http://node1:9870", user="hdfs")

    # 分割文件并上传到HDFS
    split_file(file_path, 3, hdfs_output_dir, hdfs_client)

    # 读取分割后的文件并输出统计
    sDB = sc.wholeTextFiles(f"hdfs://node1:8020{hdfs_output_dir}")
    print(sDB.count())

if __name__ == '__main__':
    main()

    # 停止SparkContext
    sc.stop()
