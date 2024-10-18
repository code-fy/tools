import sys
import time

ftrl_model_path = "/Users/apple/WorkSpace/tools/fm_models_tool/CL_IPR_ALLINONE.txt"
online_model_path = "./CL_IPR_ALLINONE"
model_name = "CL_IPR_ALLINONE"
push_version = "20241017"
model_meta = {
    "intercept": 0,
    "auc": 0.9,
    "logloss": 0,
    "aucTrain": 0,
    "loglossTrain": 0,
    "totalCount": 0,
    "positiveCount": 0,
    "iter": 100,
    "regParam": 0,
    "tol": 0,
    "elasticNetParam": 0.0,
    "featureCount": 0,
    "minFeatureCount": 3,
    "numberOfPartitions": 200,
    "modelVersion": model_name,
    "gloveModelVersion": "GV_BASE_202310091901",
    "time": int(time.time() * 1000),
    "seps": "\001\002\003",
    "pushVersion": f'{model_name}-{push_version}',
    "goalType": 'PAY',
    "dims":'8',
    "discretizationVersion": '2',
}
newlines = []

with open(ftrl_model_path) as f:
    next(f)  # 跳过第一行
    for ln in f:
        segs = ln.strip().split('\001')
        weight = float(segs[4])  # 将weight转为浮点数以便排序
        # embeding = segs[5].replace("\003", ",")
        fea = segs[0]

        # 添加 (fea, weight, embeding) 到列表中
        newlines.append((fea, weight))

# 按weight的绝对值排序
newlines.sort(key=lambda x: abs(x[1]), reverse=True)

# 根据排序后的列表生成新的行
sorted_lines = []
for fea, weight in newlines:
    sorted_lines.append(f'{fea}****{weight}\n')

# 输出或写入文件
with open('sorted_ftrl_model_CL_IPR_ALLINONE.txt', 'w') as out_file:
    out_file.writelines(sorted_lines)



# ./to_online_fmt.py /home/ubuntu/data/dsp-fm-modelpipeline/dsp-modelpipeline/data/dsp_models/20241016213001 ./CL_IPR_ALLINONE.txt CL_IPR_ALLINONE 20241017003001