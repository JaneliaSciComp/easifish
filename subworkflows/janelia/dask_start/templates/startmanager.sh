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

manager_exit_code=0

# cleanup running process
function cleanup() {
    if [[ -f "\${scheduler_pid_file}" ]]; then
        local dpid
        dpid=\$(cat "\${scheduler_pid_file}" || true)
        if [[ -n "\${dpid}" ]] && kill -0 "\${dpid}" 2>/dev/null; then
            kill -TERM "\${dpid}" 2>/dev/null || true
            local i=0
            while (( i < 10 )) && kill -0 "\${dpid}" 2>/dev/null; do
                sleep 1
                i=\$(( i + 1 ))
            done
            kill -9 "\${dpid}" 2>/dev/null || true
        fi
    fi
}

# INT/TERM: just log; the waitforanyfile.sh calls use || true so the script
# continues to cleanup and the Nextflow epilogue naturally.
function on_term() {
    echo "Received termination signal, stopping scheduler"
}

# EXIT: runs after the epilogue and versions.yml have been written.
function on_exit() {
    cleanup
    exit \${manager_exit_code}
}

trap on_term INT TERM
trap on_exit EXIT

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

# wait for PID file (or terminate signal); || true so a signal-killed
# subprocess does not trip set -e before the epilogue.
${moduleDir}/templates/waitforanyfile.sh 0 "\${terminate_file_name},\${scheduler_pid_file}" || true

if [[ -e "\${scheduler_pid_file}" ]]; then
    scheduler_pid=\$(cat "\${scheduler_pid_file}")
    echo "Scheduler started: pid=\$scheduler_pid"
    echo "Wait for termination event: \${terminate_file_name}"
    ${moduleDir}/templates/waitforanyfile.sh \${scheduler_pid} "\${terminate_file_name}" || true
else
    echo "Scheduler pid file not found"
    scheduler_pid=0
fi

echo "Killing dask scheduler"
cleanup

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
