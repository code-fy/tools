import pandas as pd
import subprocess


# 读取聚类结果文件
file_path = 'clustered_features.txt'
data = pd.read_csv(file_path, sep='\t')

# 统计每个cluster中feature的个数
cluster_counts = data.groupby('cluster')['feature'].count().reset_index()

# 输出统计结果
print(cluster_counts)

# 如果你想将结果保存为新文件，可以使用：
cluster_counts.to_csv('cluster_counts.txt', index=False, sep='\t')

# 定义排序命令
sort_command = "sort -t $'\\t' -k2,2n -r cluster_counts.txt -o sorted_clustered_features.txt"

# 使用subprocess调用shell命令
try:
    subprocess.run(sort_command, shell=True, check=True)
    print("文件已成功排序并保存到 sorted_clustered_features.txt")
except subprocess.CalledProcessError as e:
    print(f"排序失败: {e}")