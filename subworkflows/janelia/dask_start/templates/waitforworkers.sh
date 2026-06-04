#!/bin/bash -ue
# Wait for the required number of Dask workers to register with the scheduler.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
cluster_work_fullpath=\$(\${READLINK_TOOL} ${cluster_work_dir})
terminate_file_name="\${cluster_work_fullpath}/terminate-dask"

declare -i worker_start_timeout=${task.ext.worker_start_timeout ?: '-1'}
declare -i worker_poll_interval=${task.ext.worker_poll_interval ?: '1'}
declare -i total_workers=${total_workers}
declare -i required_workers=${required_workers}

if (( \${required_workers} > 0 )); then
    seconds=0
    while true; do
        if [[ -e \${terminate_file_name} ]]; then
            # this can happen if the cluster is created on LSF and the workers cannot get nodes
            # before the cluster is ended
            available_workers=-1
            exit 1
        fi
        available_workers=0
        for (( worker_id=1; worker_id<=\${total_workers}; worker_id++ )); do
            worker_name="worker-\${worker_id}"
            worker_log="\${cluster_work_fullpath}/\${worker_name}/\${worker_name}.log"
            # if worker's log exists check if the worker has connected to the scheduler
            echo "Check \${worker_log}"
            if [[ -e "\${worker_log}" ]]; then
                found=\$(grep -o "Registered to:.*${scheduler_address}" \${worker_log} || true)
                if [[ ! -z \${found} ]]; then
                    echo "\${found}"
                    available_workers=\$(( \${available_workers} + 1 ))
                fi
            fi
        done
        echo "Found \${available_workers} after \${seconds}"
        # in case somebody forgets to adjust the required workers check also if it is equal to total_workers
        if (( \${available_workers} >= \${total_workers} || \${available_workers} >= \${required_workers} )); then
            echo "Found \${available_workers} connected workers"
            break
        fi
        if (( \${worker_start_timeout} > 0 && \${seconds} > \${worker_start_timeout} )); then
            echo "Timed out after \${seconds} seconds while waiting for at least \${required_workers} workers to connect to scheduler"
            available_workers=-1
            exit 2
        fi
        sleep \${worker_poll_interval}
        seconds=\$(( \${seconds} + \${worker_poll_interval} ))
    done
else
    available_workers=-1
fi

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
