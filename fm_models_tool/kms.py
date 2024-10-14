from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
from sklearn.cluster import MiniBatchKMeans
chunk_size = 100000
file_path = '/Users/apple/WorkSpace/model_training/CL_FM_NC.txt'
kmeans = MiniBatchKMeans(n_clusters=10000, random_state=0)
# 假设embedding的维度较高，可以先降维
# pca = PCA(n_components=50)  # 降到50维
# 统计处理的批次数
batch_num = 0
total_rows = 0

# 保存结果的文件
output_file = 'clustered_features.txt'

# 按批次处理文件
with pd.read_csv(file_path, sep='\t', header=None, names=['feature', 'embedding'], chunksize=chunk_size) as reader:
    for chunk in reader:
        batch_num += 1
        total_rows += len(chunk)

        # 将embedding列的字符串转换为数值向量
        chunk['embedding'] = chunk['embedding'].apply(lambda x: np.array([float(i) for i in x.split(',')]))

        # 将所有embedding转成一个numpy矩阵
        embedding_matrix = np.stack(chunk['embedding'].values)

        # 进行MiniBatchK-means聚类（增量训练）
        kmeans.partial_fit(embedding_matrix)

        # 获取当前批次的聚类标签
        chunk['cluster'] = kmeans.labels_

        # 显示当前处理的批次数和已处理的样本数
        print(f'Processed batch {batch_num}, total rows processed: {total_rows}')

        # 保存当前批次的结果到文件，追加写入模式
        chunk[['feature', 'cluster']].to_csv(output_file, mode='a', header=(batch_num == 1), index=False, sep='\t')

# 聚类完成后，输出聚类中心
print("MiniBatchK-means clustering completed.")
print("Cluster centers:")
print(kmeans.cluster_centers_)

