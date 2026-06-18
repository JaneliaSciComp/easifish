#!/bin/bash -ue
# Start the Spark manager process and wait for terminate signal.

set +x

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
spark_local_tmp_dir="${spark_local_dir ? spark_local_dir : '/tmp'}"
full_spark_local_dir="\$(\${READLINK_TOOL} -m \${spark_local_tmp_dir})/spark-${workflow.sessionId}"
spark_master_log_file="\${full_spark_work_dir}/sparkmaster.log"
spark_config_filepath="\${full_spark_work_dir}/spark-defaults.conf"
terminate_file_name="\${full_spark_work_dir}/terminate-spark-${workflow.sessionId}"
args="${task.ext.args ?: ''}"
sleep_secs="${task.ext.sleep_secs ?: '1'}"

if [[ ! -e \${full_spark_work_dir} ]]; then
    echo "Create spark work directory ${spark_work_dir} -> \${full_spark_work_dir}"
    mkdir -p \${full_spark_work_dir}
else
    echo "Spark work directory: \${full_spark_work_dir} - already exists"
fi

echo "Starting spark master - logging to \${spark_master_log_file}"
rm -f \${spark_master_log_file} || true

if [[ ! -e \${spark_config_filepath} ]]; then
    # Fallback: create Spark configuration if PREPARE_SPARK_CONFIG didn't run.
    echo "Creating Spark configuration at \${spark_config_filepath}"
    mkdir -p \$(dirname \${spark_config_filepath})
    echo "# Spark config file"                         > \${spark_config_filepath}
    echo "spark.rpc.askTimeout=300s"                  >> \${spark_config_filepath}
    echo "spark.storage.blockManagerHeartBeatMs=30000" >> \${spark_config_filepath}
    echo "spark.rpc.retry.wait=30s"                   >> \${spark_config_filepath}
    echo "spark.kryoserializer.buffer.max=1024m"      >> \${spark_config_filepath}
    echo "spark.core.connection.ack.wait.timeout=600s" >> \${spark_config_filepath}
    echo "spark.driver.maxResultSize=0"               >> \${spark_config_filepath}
    echo "spark.worker.cleanup.enabled=true"          >> \${spark_config_filepath}
    echo "spark.local.dir=\${full_spark_local_dir}"   >> \${spark_config_filepath}

    echo "Created spark config file: \$(cat \${spark_config_filepath})"
else
    echo "Spark config file already exists: \$(cat \${spark_config_filepath})"
fi

# Initialize the environment for Spark
echo "Initializing Spark environment..."
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

echo "Determining manager IP address..."
. ${moduleDir}/templates/determine_ip.sh ${workflow.containerEngine}

# Start the Spark manager
echo "Spark master (\${local_ip}) output to \${spark_master_log_file}"
set -x
CMD=(
    /opt/spark/bin/spark-class
    org.apache.spark.deploy.master.Master
    -h \${local_ip}
    --properties-file \${spark_config_filepath}
    \${args}
)
echo "CMD: \${CMD[@]}"

attempt_setup_fake_passwd_entry
(exec \$(switch_user_if_root) /usr/bin/tini -s -- "\${CMD[@]}" > "\${spark_master_log_file}" 2>&1) &
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
manager_exit_code=0
terminate_requested=0

function cleanup() {
    echo "Killing background processes"
    [[ -n "\${spid:-}" ]] && kill -9 "\$spid" 2>/dev/null || true
    exit \$manager_exit_code
}
trap cleanup EXIT

function on_signal() {
    echo "Received termination signal, stopping manager"
    terminate_requested=1
}
trap on_signal INT TERM

while true; do
    if [[ \$terminate_requested -eq 1 ]]; then
        echo "Manager terminating due to signal"
        break
    fi
    if ! kill -0 \$spid >/dev/null 2>&1; then
        echo "Process \$spid died"
        cat \${spark_master_log_file} >&2
        manager_exit_code=1
        break
    fi
    if [[ -e "\${terminate_file_name}" ]]; then
        cat \${spark_master_log_file}
        break
    fi
    sleep \${sleep_secs} || true
done
