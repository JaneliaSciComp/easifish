#!/bin/bash -ue
# Start the Dask scheduler and wait for terminate signal.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
cluster_work_fullpath=\$(\${READLINK_TOOL} ${cluster_work_dir})
${dask_config ? 'export DASK_CONFIG=$(${READLINK_TOOL} ' + dask_config + ')' : ''}
args="${task.ext.args ?: ''}"
scheduler_pid_file="\${cluster_work_fullpath}/dask-scheduler.pid"
scheduler_info_file="\${cluster_work_fullpath}/dask-scheduler-info.json"
terminate_file_name="\${cluster_work_fullpath}/terminate-dask"

echo "Scheduler's environment"
env

function cleanup() {
    echo "Killing scheduler background processes"
    if [[ -f "\${scheduler_pid_file}" ]]; then
        local dpid=\$(cat "\${scheduler_pid_file}")
        kill -9 "\$dpid" || true
    fi
    exit 0
}
trap cleanup INT TERM EXIT

echo "Determining scheduler IP address..."
. ${moduleDir}/templates/determine_ip.sh ${workflow.containerEngine}

# start scheduler in background
echo "Run: dask scheduler --host \${local_ip} --pid-file \${scheduler_pid_file} --scheduler-file \${scheduler_info_file} \${args}"
dask scheduler \
    --host \${local_ip} \
    --pid-file \${scheduler_pid_file} \
    --scheduler-file \${scheduler_info_file} \
    \${args} \
    2> >(tee \${cluster_work_fullpath}/dask-scheduler.log >&2) \
    &

# wait for PID file (or terminate signal)
${moduleDir}/templates/waitforanyfile.sh 0 "\${terminate_file_name},\${scheduler_pid_file}"

if [[ -e "\${scheduler_pid_file}" ]]; then
    scheduler_pid=\$(cat "\${scheduler_pid_file}")
    echo "Scheduler started: pid=\$scheduler_pid"
    echo "Wait for termination event: \${terminate_file_name}"
    ${moduleDir}/templates/waitforanyfile.sh \${scheduler_pid} "\${terminate_file_name}"
else
    echo "Scheduler pid file not found"
    scheduler_pid=0
fi

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
