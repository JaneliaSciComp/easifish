#!/bin/bash -ue
# Start a Dask worker and wait for terminate signal.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
cluster_work_fullpath=\$(\${READLINK_TOOL} ${cluster_work_dir})
${dask_config ? 'export DASK_CONFIG=$(${READLINK_TOOL} ' + dask_config + ')' : ''}
args="${task.ext.args ?: ''}"
worker_name="worker-${worker_id}"
worker_dir="\${cluster_work_fullpath}/\${worker_name}"
worker_pid_file="\${worker_dir}/\${worker_name}.pid"
terminate_file_name="\${cluster_work_fullpath}/terminate-dask"
memory_limit="${worker_mem_in_gb / worker_cpus * (worker_cpus + task.attempt - 1) as int}G"

echo "Worker's environment"
env

worker_exit_code=0

# cleanup running process
function cleanup() {
    if [[ -f "\${worker_pid_file}" ]]; then
        local wpid
        wpid=\$(cat "\${worker_pid_file}" || true)
        if [[ -n "\${wpid}" ]] && kill -0 "\${wpid}" 2>/dev/null; then
            kill -TERM "\${wpid}" 2>/dev/null || true
            local i=0
            while (( i < 10 )) && kill -0 "\${wpid}" 2>/dev/null; do
                sleep 1
                i=\$(( i + 1 ))
            done
            kill -9 "\${wpid}" 2>/dev/null || true
        fi
    fi
}

# INT/TERM: just log; the waitforanyfile.sh calls use || true so the script
# continues to cleanup and the Nextflow epilogue naturally.
function on_term() {
    echo "Received termination signal, stopping worker \${worker_name}"
}

# EXIT: runs after the epilogue and versions.yml have been written.
function on_exit() {
    cleanup
    exit \${worker_exit_code}
}

trap on_term INT TERM
trap on_exit EXIT

echo "Determining worker \${worker_name} IP address..."
. ${moduleDir}/templates/determine_ip.sh ${workflow.containerEngine}

mkdir -p \${worker_dir}

# start worker in background
echo "Run: dask worker --host \${local_ip} --local-directory \${worker_dir} --name \${worker_name} --pid-file \${worker_pid_file} --memory-limit \${memory_limit} \${args} ${scheduler_address}"
dask worker \
    --host \${local_ip} \
    --local-directory \${worker_dir} \
    --name \${worker_name} \
    --pid-file \${worker_pid_file} \
    --memory-limit \${memory_limit} \
    \${args} \
    ${scheduler_address} \
    2> >(tee \${worker_dir}/\${worker_name}.log >&2) \
    &

# wait for PID file (or terminate signal); || true so a signal-killed
# subprocess does not trip set -e before the epilogue.
${moduleDir}/templates/waitforanyfile.sh 0 "\${terminate_file_name},\${worker_pid_file}" || true

if [[ -e "\${worker_pid_file}" ]]; then
    worker_pid=\$(cat "\${worker_pid_file}")
    echo "Worker \${worker_name} started: pid=\$worker_pid"
    echo "Worker \${worker_name} - wait for termination event: \${terminate_file_name}"
    ${moduleDir}/templates/waitforanyfile.sh \${worker_pid} "\${terminate_file_name}" || true
else
    echo "Worker \${worker_name} pid file not found"
    worker_pid=0
fi

echo "Killing background processes for \${worker_name}"
cleanup

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
