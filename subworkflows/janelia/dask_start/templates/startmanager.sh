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

# The trap pattern below preserves Nextflow's output-capture epilogue
# (versions.yml + any appended .command.env writers). If the trap calls exit
# inside an INT/TERM handler, the script terminates before that epilogue runs
# and Nextflow reports missing outputs (".command.env not found" or missing
# versions.yml).
#
# EXIT trap: kills the scheduler and applies the recorded exit code.
#            Runs AFTER the epilogue, so outputs are written first.
# INT/TERM trap: just sets a flag; the wait calls below absorb the signal-
#                induced non-zero exit via "|| true" and let the script flow.
manager_exit_code=0
terminate_requested=0

function cleanup() {
    echo "Killing scheduler background processes"
    if [[ -f "\${scheduler_pid_file}" ]]; then
        local dpid
        dpid=\$(cat "\${scheduler_pid_file}" 2>/dev/null || true)
        [[ -n "\${dpid}" ]] && kill -9 "\$dpid" 2>/dev/null || true
    fi
    exit \$manager_exit_code
}
trap cleanup EXIT

function on_signal() {
    echo "Received termination signal, stopping scheduler"
    terminate_requested=1
}
trap on_signal INT TERM

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

if [[ \$terminate_requested -eq 1 ]]; then
    echo "Scheduler terminating due to signal"
    scheduler_pid=0
elif [[ -e "\${scheduler_pid_file}" ]]; then
    scheduler_pid=\$(cat "\${scheduler_pid_file}")
    echo "Scheduler started: pid=\$scheduler_pid"
    echo "Wait for termination event: \${terminate_file_name}"
    ${moduleDir}/templates/waitforanyfile.sh \${scheduler_pid} "\${terminate_file_name}" || true
else
    echo "Scheduler pid file not found"
    scheduler_pid=0
fi

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
