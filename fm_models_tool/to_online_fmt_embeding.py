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
seps = model_meta['seps']
for ln in open(ftrl_model_path):
    segs = ln.strip().split('\002')
    weight = segs[1]
    if weight == '0.0':
        continue
    fea = segs[0]
    fkey = []
    fvalue = []
    fembeding = segs[3]
    for kv in fea.split('\001'):
        fsgs = kv.split('=')
        fk = fsgs[0]
        fv = '='.join(fsgs[1:])
        if fk == 'latitude' or fk == 'longitude':
            fv = str(int(float(fv)))
        if fk == 'size':
            fk = 'creative_size'
        fkey.append(fk)
        fvalue.append(fv)
    fkey = seps[2].join(fkey)
    fvalue = seps[2].join(fvalue)
    newlines.append(seps[0].join(
        [f"{fkey}{seps[1]}{fvalue}", '0', '0', '0', f'{weight}{seps[0]}{fembeding}']) + '\n')
with open(online_model_path, 'w') as wf:
    model_meta['featureCount'] = len(newlines)
    meta_line = '\t'.join([f'{k}:{v}' for k, v in model_meta.items()])
    wf.write(meta_line + '\n')
    wf.writelines(newlines)

# ./to_online_fmt.py /home/ubuntu/data/dsp-fm-modelpipeline/dsp-modelpipeline/data/dsp_models/20241016213001 ./CL_IPR_ALLINONE.txt CL_IPR_ALLINONE 20241017003001