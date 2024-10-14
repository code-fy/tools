import logging
import sys
import threading
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

# 配置日志记录
logging.basicConfig(level=logging.INFO,  # 设置日志级别为INFO
                    format='%(asctime)s - %(levelname)s - %(message)s')  # 设置日志格式
# 设置你的 Google Cloud 项目 ID
project_id = 'taurusx-sage'
region = 'us-east1'
cluster_name = 'data-check'
# 设置你的 Google Cloud 项目 ID 和 GCS 存储桶名称
project_id = project_id
bucket_name = 'dsp_alg'
folder_path = 'common/python_project/py/'  # GCS文件夹路径，如 'folder/subfolder/'
file_path = '{}successed'.format(folder_path)
# 设置创建集群的配置
cluster_config = {
    'project_id': project_id,
    'cluster_name': cluster_name,
    'config': {
        'config_bucket': 'dsp_alg_test',
        # 'initialization_actions': [
        #     {
        #         'executable_file': 'gs://dsp_alg_test/tmp_data/shells/dataproc_start_action.sh',
        #     }
        # ],
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'n2-highmem-16',
            'disk_config': {
                'boot_disk_size_gb': 300,
            }
        },
        'worker_config': {
            'num_instances': 2,  # Set the desired number of worker instances
            'machine_type_uri': 'n2-highmem-16',
            'disk_config': {
                'boot_disk_size_gb': 500,
            }
        },
        'secondary_worker_config': {
            'num_instances': 0,  # Set the desired number of secondary worker instances
            'machine_type_uri': 'n2-highmem-16',
            'disk_config': {
                'boot_disk_size_gb': 50,
            }
        },
        'software_config': {
            'optional_components': ['JUPYTER', 'HUDI'],
            'image_version': '2.0',
        },
        'gce_cluster_config': {
            'metadata': {
                'spark-worker-instance': 'preemptible'  # 添加此键值对以启用抢占式实例
            },
        },
        'lifecycle_config': {
            'idle_delete_ttl': '3000s',
        },
        'endpoint_config': {
            'enable_http_port_access': True,
        },
    },
}
# 创建 GCS 客户端
gcs_client = storage.Client(project=project_id)
# 创建 Dataproc 客户端
job_client = dataproc.JobControllerClient(client_options={
    'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
})
cluster_client = dataproc.ClusterControllerClient(
    client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
)


def get_config_folders(client):
    # 获取指定文件夹下的所有文件
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    # 生成文件名列表
    file_names = [blob.name for blob in blobs]
    return file_names


def submit_jobs(client, cluster_name, job_args, project_id, region, event):
    # 设置你的 Google Cloud 项目 ID
    project_id = project_id
    # 设置 Dataproc 集群名称和区域
    cluster_name = cluster_name
    region = region
    # 设置 Dataproc 作业的名称和入口脚本路径
    job_name = 'pyspark-job'
    entry_script = 'gs://dsp_alg/common/python_project/py_test_code/start.py'
    # 设置 JAR 文件路径
    jar_path = 'gs://dsp-bigdata/runtime/jars/postgresql-42.6.0.jar'
    # 设置要传递给 PySpark 作业的参数
    job_args = [job_args]
    # 设置要传递给 PySpark 作业的文件列表
    files = [
        'gs://dsp_alg/common/python_project/py_test_code/start.py',
        'gs://dsp_alg/common/python_project/py_test_code/data_gen.py',
        'gs://dsp_alg/common/python_project/online_model_py/feature_eng.py',
        'gs://dsp_alg/common/python_project/py_test_code/config_init.py',
        'gs://dsp_alg/common/python_project/online_model_py/init_spark.py',
        'gs://dsp_alg/common/python_project/py_test_code/train_xgb.py',
        'gs://dsp_alg/common/python_project/online_model_py/train_lr.py',
        'gs://dsp_alg/common/python_project/online_model_py/metric_funcs.py'
    ]

    # 创建 Dataproc 客户端
    # client = dataproc.JobControllerClient(client_options={
    #     'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    # })

    # 构造 Dataproc 集群的路径
    # cluster_path = client.cluster_path(project_id, region, cluster_name)

    # 构造 Dataproc 作业的配置
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': entry_script,
            'jar_file_uris': [jar_path],
            'args': job_args,
            'file_uris': files
        }
    }

    # 提交 Dataproc 作业
    response = client.submit_job(
        project_id=project_id,
        region=region,
        job=job
    )

    # 等待作业完成
    # result = operation.result()
    logging.info(f"Job {response.reference.job_id} has status {response.status.state}")
    logging.info(f"Please go to dataproc check Job: {response.reference.job_id}")
    event.set()


def execute(configs, event):
    threads = []
    for config in configs:
        logging.info("{} model is training".format(config.split(".")[0]))
        thread = threading.Thread(target=submit_jobs,
                                  args=(job_client, cluster_name, config, project_id, region, event))
        # submit_jobs(job_client, cluster_name, config, project_id, region, event)
        # time.sleep(60)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    # 等待所有线程完成后再执行 blob.delete()
    # completion_event.wait()


if __name__ == '__main__':

    blob = gcs_client.bucket(bucket_name).blob(file_path)
    configs_name = []
    folders = get_config_folders(gcs_client)
    for folder in folders:
        if folder.endswith(".ini"):
            fold = folder.split("/")[-1]
            configs_name.append(fold)
    # 创建事件对象
    completion_event = threading.Event()
    # 获取集群上的所有作业
    try:
        cluster_client.get_cluster(project_id=project_id,
                                   region=region,
                                   cluster_name=cluster_name)
        logging.info(f"{cluster_name} exists get all jobs status.")

        request = dataproc.ListJobsRequest(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name
        )
        jobs_list = job_client.list_jobs(request=request)

        # 打印每个作业的状态
        job_status = []
        for job in jobs_list.jobs:
            logging.info(f"Job {job.reference.job_id} has status {job.status.state}")
            job_status.append(job.status.state)
        if 2 in job_status:
            logging.info("some jobs are running, please hold")
            sys.exit()
        else:
            logging.info(f"Cluster {cluster_name} already exists.")
            execute(configs_name, completion_event)

    except Exception as e:
        # logging.error(f"Error: {e}")
        # 集群不存在，创建新的集群
        logging.info(f"No big deal Cluster {cluster_name} does not exist. Creating a new cluster.")
        # 提交创建集群的请求
        operation = cluster_client.create_cluster(
            project_id=project_id,
            region=region,
            cluster=cluster_config
        )

        # 等待集群创建完成
        logging.info(f"Cluster {cluster_name} created successfully.")
        execute(configs_name, completion_event)
