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
terminate_file_name="\${full_spark_work_dir}/terminate-spark-${workflow.sessionId}"
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

# The trap pattern below preserves Nextflow's env-capture epilogue:
# Nextflow appends the lines that write .command.env to the END of this script,
# so the wait loop must always reach the end of the file. Calling exit from
# inside an INT/TERM trap (or via exit 1 in the loop) bypasses that epilogue
# and Nextflow then reports ".command.env not found".
#
# EXIT trap: kills the Spark process and applies the recorded exit code.
#            Runs AFTER the env-capture epilogue, so .command.env is written
#            first and the recorded exit code is still propagated.
# INT/TERM trap: just sets a flag; the wait loop breaks cleanly on next tick.
worker_exit_code=0
terminate_requested=0

function cleanup() {
    echo "Killing background processes"
    [[ -n "\${spid:-}" ]] && kill -9 "\$spid" 2>/dev/null || true
    exit \$worker_exit_code
}
trap cleanup EXIT

function on_signal() {
    echo "Received termination signal, stopping worker"
    terminate_requested=1
}
trap on_signal INT TERM

while true; do
    if [[ \$terminate_requested -eq 1 ]]; then
        echo "Worker terminating due to signal"
        break
    fi
    if ! kill -0 \$spid >/dev/null 2>&1; then
        echo "Process \$spid died"
        cat \${spark_worker_log_file} >&2
        worker_exit_code=1
        break
    fi
    if [[ -e "\${terminate_file_name}" ]]; then
        cat \${spark_worker_log_file}
        break
    fi
    sleep \${sleep_secs} || true
done
