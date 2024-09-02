set -x
LOCAL_DIR=/opt/nfs/wxl
LD_LIBRARY_PATH=${LOCAL_DIR}/chukonu_install/lib:${LD_LIBRARY_PATH}
LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2
CHUKONU_INSTALL=${LOCAL_DIR}/chukonu_install
CHUKONU_JAR=${CHUKONU_INSTALL}/jar/chukonu_2.12-0.5.1.jar
CHUKONU_STAGING=${LOCAL_DIR}/chukonu_staging
CHUKONU_TMP=${LOCAL_DIR}/chukonu_tmp
CHUKONU_CACHE=${LOCAL_DIR}/chukonu_cache
CHUKONU_BUILD_TYPE="Release"
CHUKONU_GPP=/usr/bin/g++
MASTER="yarn"
SPARKSUBMIT="spark-submit"
offHeapSize="10g"
maxResultSize="15g"
driverMemory="50g"
WaitingTime="36000s"
JAVA8=/usr/lib/jvm/java-1.8.0-openjdk-amd64
JAVA11=/usr/lib/jvm/java-11-openjdk-amd64
minRegisteredResourcesRatio="1.0"
memoryOverheadFactor="0.5"
storageFraction="0.1"
shuffleFraction="0.1"

CORES=12
EXECUTORS=3
MEMORYS=5g
CONF_ARGS="--deploy-mode client \
        --packages com.github.scopt:scopt_2.12:3.7.1,io.delta:delta-core_2.12:2.4.0 \
        --conf spark.executorEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH} \
        --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH} \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --num-executors ${EXECUTORS}  --executor-cores ${CORES}  --executor-memory ${MEMORYS} \
        --conf spark.shuffle.memoryFraction=${shuffleFraction} \
        --conf spark.storage.memoryFraction=${storageFraction} \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.service.enabled=true \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=${WaitingTime} \
        --conf spark.scheduler.minRegisteredResourcesRatio=${minRegisteredResourcesRatio} \
        --conf spark.driver.maxResultSize=${maxResultSize} \
        --conf spark.memory.offHeap.enabled=true \
	--conf spark.driver.memory=${driverMemory} \
        --conf spark.memory.offHeap.size=${offHeapSize} "

CHUKONU_CONF_ARGS="--deploy-mode client \
        --packages com.github.scopt:scopt_2.12:3.7.1,io.delta:delta-core_2.12:2.4.0 \
        --conf spark.executorEnv.JAVA_HOME=${JAVA11} \
        --conf spark.yarn.appMasterEnv.JAVA_HOME=${JAVA11} \
        --conf spark.executorEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH} \
        --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH} \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --num-executors ${EXECUTORS}  --executor-cores ${CORES}  --executor-memory ${MEMORYS} \
        --conf spark.shuffle.memoryFraction=${shuffleFraction} \
        --conf spark.storage.memoryFraction=${storageFraction} \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.service.enabled=true \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=${WaitingTime} \
        --conf spark.scheduler.minRegisteredResourcesRatio=${minRegisteredResourcesRatio} \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=${offHeapSize} \
        --conf spark.kryo.unsafe=true \
        --conf spark.executor.processTreeMetrics.enabled=true \
        --conf spark.plugins=org.pacman.chukonu.ChukonuPlugin \
        --conf spark.chukonu.enableNativeCodegen=true \
        --conf spark.chukonu.root=${CHUKONU_INSTALL} \
        --conf spark.chukonu.cxx=${CHUKONU_GPP} \
        --conf spark.chukonu.stagingdir=${CHUKONU_STAGING} \
        --conf spark.chukonu.compileCacheDir=${LOCAL_DIR}/chukonu_install/ccnet_cache \
        --conf spark.chukonu.buildType=Release \
        --conf spark.executorEnv.LD_LIBRARY_PATH=${CHUKONU_INSTALL}/lib:${LD_LIBRARY_PATH} \
        --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=${CHUKONU_INSTALL}/lib:${LD_LIBRARY_PATH} \
        --conf spark.executorEnv.LD_PRELOAD=${CHUKONU_INSTALL}/lib/libchukonu_preloaded.so:${LD_PRELOAD} \
        --conf spark.yarn.appMasterEnv.LD_PRELOAD=${CHUKONU_INSTALL}/lib/libchukonu_preloaded.so:${LD_PRELOAD} \
        --conf spark.yarn.max.executor.failures=10000 \
        --conf spark.driver.maxResultSize=${maxResultSize} \
	--conf spark.driver.memory=${driverMemory} \
        --conf spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=512MB \
        --jars ${CHUKONU_JAR} "
export LD_PRELOAD=${LOCAL_DIR}/chukonu_install/lib/libchukonu_preloaded.so:${LD_PRELOAD}
# 参数判断
if [ "$1" == "use_chu" ]; then
    SPARK_CONF_ARGS=${CHUKONU_CONF_ARGS}
else
    SPARK_CONF_ARGS=${CONF_ARGS}
fi

# 提交 Spark 作业
${SPARKSUBMIT} --master ${MASTER} ${SPARK_CONF_ARGS} $2
