#!/bin/sh

set -ex
# 修改工程名称，自定义
PROJECT_NAME=ccnet_spark
# 任务名称，自定义
JOB_NAME=demo-spark

K8S_URL=https://172.31.13.217:6443
# 后续没人一个命名空间
K8S_USER=spark
K8S_NAMESPACE=spark

# 上传路径，保持不变
UPLOAD_PATH=/data0/k8s/node0_data/spark-job/$PROJECT_NAME
# 程序路径，自定义
WORKDIR=/root/wxl_folder/$PROJECT_NAME
# 程序路径，自定义
MAIN_PATH=$WORKDIR/test3/run_dis.py

# 数据本地路径
LOCAL_DIR_0_PATH=/data0/k8s/node0_data
# 数据本地子路径 ， 和LOCAL_DIR_0_PATH 拼接成完成本地目录
LOCAL_DIR_0_SUB_PATH=ccnet_spark
# 容器内目录，程序中读取数据使用该目录，和上面的 "LOCAL_DIR_0_PATH/LOCAL_DIR_0_SUB_PATH" 路径映射
LOCAL_DIR_0_MOUNT_PATH=/opt/spark/work-dir/data


#spark 参数，自定义
DRIVER_MEMORY="5g"
MAX_RESULT_SIZE="5g"
PARALLELISM=30
NUM_EXECUTORS=12
EXECUTOR_CORES=2
EXECUTOR_MEMORY="10g"
OFFSIZE="1g"

# spark 镜像保持不变，如果要推送镜像，请不要使用3.4.1版本，例如可以使用 wangshd-3.4.1
IMAGE=registry.cn-hangzhou.aliyuncs.com/houdu_bigdata/spark-py:3.4.1-ccnet-1.0
# 镜像仓库凭证保持不变
IMAGE_PULLSECRETS=registrykey
# 保持不变
SPARK_PATH=/opt/spark
# 保持不变
SPARK_HOME=/opt/spark


$SPARK_PATH/bin/spark-submit \
    --master k8s://$K8S_URL \
    --deploy-mode cluster \
    --name $JOB_NAME \
    --conf spark.kubernetes.namespace=$K8S_USER \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=$K8S_NAMESPACE \
    --conf spark.kubernetes.container.image=$IMAGE \
    --conf spark.kubernetes.container.image.pullSecrets=$IMAGE_PULLSECRETS \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.file.upload.path=$UPLOAD_PATH \
    --conf spark.kubernetes.driver.volumes.hostPath.local-dir-0.options.path=$LOCAL_DIR_0_PATH \
    --conf spark.kubernetes.driver.volumes.hostPath.local-dir-0.mount.subPath=$LOCAL_DIR_0_SUB_PATH \
    --conf spark.kubernetes.driver.volumes.hostPath.local-dir-0.mount.path=$LOCAL_DIR_0_MOUNT_PATH \
    --conf spark.kubernetes.executor.volumes.hostPath.local-dir-0.options.path=$LOCAL_DIR_0_PATH \
    --conf spark.kubernetes.executor.volumes.hostPath.local-dir-0.mount.subPath=$LOCAL_DIR_0_SUB_PATH \
    --conf spark.kubernetes.executor.volumes.hostPath.local-dir-0.mount.path=$LOCAL_DIR_0_MOUNT_PATH \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.options.path=/data0/k8s/node0_data \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.mount.subPath=spark-job \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.mount.path=/data0/k8s/node0_data/spark-job \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.options.path=/data0/k8s/node0_data \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.mount.subPath=spark-job \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.mount.path=/data0/k8s/node0_data/spark-job \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///opt/spark/eventLog \
    --conf spark.history.fs.logDirectory=file:///opt/spark/eventLog \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-2.options.path=/data0/k8s/node1_data \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-2.mount.subPath=spark-history/eventLog \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-2.mount.path=/opt/spark/eventLog \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-3.options.path=/data0/k8s/node1_data \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-3.mount.subPath=spark-history/logs \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir-3.mount.path=/opt/spark/logs \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-3.options.path=/data0/k8s/node1_data \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-3.mount.subPath=spark-history/logs \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-3.mount.path=/opt/spark/logs \
    --conf spark.kubernetes.driverEnv.TZ=Asia/Shanghai \
    --conf spark.kubernetes.executorEnv.TZ=Asia/Shanghai \
    --conf spark.kubernetes.driverEnv.SPARK_HOME=$SPARK_HOME \
    --conf spark.kubernetes.executorEnv.SPARK_HOME=$SPARK_HOME \
    --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp \
    --conf spark.driver.extraJavaOptions=-Divy.home=/tmp \
    --conf spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false \
    --conf spark.hadoop.spark.sql.parquet.binaryAsString=false \
    --conf spark.hadoop.spark.sql.parquet.int96AsTimestamp=true \
    --conf spark.hadoop.spark.sql.caseSensitive=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=36000s \
    --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=$OFFSIZE \
    --conf spark.kryo.unsafe=true \
    --conf spark.executor.processTreeMetrics.enabled=true \
    --conf spark.driver.maxResultSize=$MAX_RESULT_SIZE \
    --conf spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=512MB \
    --conf spark.driver.memory=$DRIVER_MEMORY \
    --conf spark.default.parallelism=$PARALLELISM \
    --num-executors $NUM_EXECUTORS \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEMORY \
    --py-files $MAIN_PATH $MAIN_PATH
