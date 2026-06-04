#!/bin/bash -ue
# Run the given Spark application, either on the cluster
# or standalone (if spark.uri == "local[*]").

set +x
set -eo pipefail

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
spark_config_filepath="\${full_spark_work_dir}/spark-defaults.conf"

echo "Starting Spark driver with main class ${app_main_class}"

# Initialize the environment for Spark
export SPARK_ENV_LOADED=
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHONPATH_SET=
export PYTHONPATH="/opt/spark/python"
export SPARK_LOG_DIR="\${full_spark_work_dir}"
set +u
. "/opt/spark/sbin/spark-config.sh"
. "/opt/spark/bin/load-spark-env.sh"
set -u

. ${moduleDir}/templates/userutils.sh

if [[ "${spark.uri}" == "local[*]" ]]; then
    spark_cluster_params=()
else
    . ${moduleDir}/templates/determine_ip.sh ${workflow.containerEngine}
    spark_cluster_params=(
        --properties-file \${spark_config_filepath}
        --conf "spark.driver.host=\${local_ip}"
        --conf "spark.driver.bindAddress=\${local_ip}"
    )
fi

# The default (4MB) open cost consolidates files into tiny partitions regardless
# of the number of cores. By forcing this parameter to zero, we can specify the
# exact parallelism that we want.
if (( ${spark.parallelism} > 0 )); then
    parallelism_conf=(--conf spark.default.parallelism=${spark.parallelism})
else
    parallelism_conf=()
fi

set -x

CMD=(
    /opt/spark/bin/spark-class
    org.apache.spark.deploy.SparkSubmit
    "\${spark_cluster_params[@]}"
    --master ${spark.uri}
    --class ${app_main_class}
    --conf spark.files.openCostInBytes=0
    "\${parallelism_conf[@]}"
    --executor-cores ${spark.executor_cpus}
    --executor-memory "${executor_memory_gb}g"
    --driver-cores ${spark.driver_cpus}
    --driver-memory "${driver_memory_gb}g"
    --conf "spark.driver.extraClassPath=${app_jar}"
    --conf "spark.executor.extraClassPath=${app_jar}"
    --conf "spark.jars.ivy=\${full_spark_work_dir}"
    --conf "spark.driver.extraJavaOptions=-Dnative.libpath.verbose=true"
    ${app_spark_conf_args}
    "${app_jar}"
    ${app_args ? app_args.join(' ') : ''}
)

echo "CMD: \${CMD[@]}"

attempt_setup_fake_passwd_entry
(exec \$(switch_user_if_root) /usr/bin/tini -s -- "\${CMD[@]}")

set +x
