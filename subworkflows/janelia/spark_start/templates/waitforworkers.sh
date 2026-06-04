#!/bin/bash -ue
# Wait for the required number of Spark workers to register with the master.

set +x

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
terminate_file_name="\${full_spark_work_dir}/terminate-spark"

declare -i worker_start_timeout=${task.ext.max_wait_secs ?: '3600'}
declare -i worker_poll_interval=${task.ext.sleep_secs ?: '1'}
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
            worker_logfile="sparkworker-\${worker_id}.log"
            worker_logpath="\${full_spark_work_dir}/\${worker_logfile}"
            # if worker's log exists check if the worker has connected to the scheduler
            echo "Check \${worker_logpath}"
            if [[ -e "\${worker_logpath}" ]]; then
                found=\$(grep -o "\\(Worker: Successfully registered with master ${spark.uri}\\)" \${worker_logpath} || true)
                if [[ ! -z \${found} ]]; then
                    echo "\${worker_logpath}: \${found}"
                    available_workers=\$(( \${available_workers} + 1 ))
                fi
            fi
        done
        echo "Found \${available_workers} workers after \${seconds} secs"
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

echo "\${available_workers}" > "\${full_spark_work_dir}/available_workers.txt"
available_workers=\$(cat "\${full_spark_work_dir}/available_workers.txt")
