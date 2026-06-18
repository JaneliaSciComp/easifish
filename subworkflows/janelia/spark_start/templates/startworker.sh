#!/bin/bash -ue
# Start a Spark worker process and wait for terminate signal.

set +x

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
spark_worker_log_file="\${full_spark_work_dir}/sparkworker-${worker_id}.log"
spark_config_filepath="\${full_spark_work_dir}/spark-defaults.conf"
terminate_file_name="\${full_spark_work_dir}/terminate-spark"
args="${task.ext.args ?: ''}"
sleep_secs="${task.ext.sleep_secs ?: '1'}"

echo "Starting spark worker ${worker_id} - logging to \${spark_worker_log_file}"
rm -f \${spark_worker_log_file} || true

# Initialize the environment for Spark
echo "Initializing Spark environment..."
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=30 -Dspark.worker.cleanup.appDataTtl=1 -Dspark.port.maxRetries=64"
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

echo "Determining worker IP address..."
. ${moduleDir}/templates/determine_ip.sh ${workflow.containerEngine}

# Start the Spark worker
set -x
CMD=(
    /opt/spark/bin/spark-class
    org.apache.spark.deploy.worker.Worker
    "${spark.uri}"
    -c "${spark.worker_cpus}"
    -m "${spark.worker_memory}G"
    -d "\${full_spark_work_dir}"
    -h "\${local_ip}"
    --properties-file "\${spark_config_filepath}"
    \${args}
)

echo "CMD: \${CMD[@]}"

attempt_setup_fake_passwd_entry
(exec \$(switch_user_if_root) /usr/bin/tini -s -- "\${CMD[@]}" > "\${spark_worker_log_file}" 2>&1) &
spid=\$!
set +x

# Ensure that Spark process dies if this script is interrupted
function cleanup() {
    echo "Killing background processes"
    [[ \$spid ]] && kill -9 "\$spid"
}
trap cleanup INT TERM EXIT

while true; do
    if ! kill -0 \$spid >/dev/null 2>&1; then
        echo "Process \$spid died"
        cat \${spark_worker_log_file} >&2
        exit 1
    fi
    if [[ -e "\${terminate_file_name}" ]]; then
        cat \${spark_worker_log_file}
        break
    fi
    sleep \${sleep_secs}
done
