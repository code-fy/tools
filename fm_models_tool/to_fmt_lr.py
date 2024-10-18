import sys
import time

ftrl_model_path = sys.argv[1]
online_model_path = sys.argv[2]
model_name = sys.argv[3]
push_version = sys.argv[4]
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
        [f"{fkey}{seps[1]}{fvalue}", '0', '0', '0', f'{weight}']) + '\n')
with open(online_model_path, 'w') as wf:
    model_meta['featureCount'] = len(newlines)
    meta_line = '\t'.join([f'{k}:{v}' for k, v in model_meta.items()])
    wf.write(meta_line + '\n')
    wf.writelines(newlines)
