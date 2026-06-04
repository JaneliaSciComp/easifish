#!/bin/bash -ue
# Start the Dask scheduler process and wait for terminate signal
DIR=$(cd "$(dirname "$0")"; pwd)

scheduler_address=
cluster_work_dir=
worker_id=
terminate_file=
declare -i worker_start_timeout=-1
declare -i worker_poll_interval=1

while [[ $# > 0 ]]; do
    key="$1"
    shift # past the key
    case $key in
        --cluster-work-dir)
            cluster_work_dir=$1
            shift
            ;;
        --worker-start-timeout)
            worker_start_timeout=$(($1))
            shift
            ;;
        --worker-poll-interval)
            scheduler_poll_interval=$(($1))
            shift
            ;;
        --worker-id)
            worker_id=$1
            shift
            ;;
        --terminate-file)
            terminate_file=$1
            shift
            ;;
        --scheduler-address)
            scheduler_address=$1
            shift
            ;;
        *)
            ;;
    esac
done

seconds=0
worker_name="worker-${worker_id}"
worker_log="${cluster_work_dir}/${worker_name}/${worker_name}.log"

echo "Worker ${worker_name} log: ${worker_log}"
while true; do
    if [[ -e ${terminate_file} ]] ; then
        # this can happen if the cluster is created on LSF and the workers cannot get nodes
        # before the cluster is ended
        echo "Termination signal: ${terminate_file} found while waiting for ${worker_name}"
        exit 1
    fi
    # if worker's log exists check if the worker has connected to the scheduler
    if [[ -e "${worker_log}" ]]; then
        found=`grep -o "Registered to:.*${scheduler_address}" ${worker_log} || true`
        if [[ ! -z ${found} ]]; then
            echo "${found}"
            echo "Finished waiting for ${worker_name} to connect"
            exit 0;
        fi
    fi
    if (( ${worker_start_timeout} > 0 && ${seconds} > ${worker_start_timeout} )); then
        echo "Timed out after ${seconds} seconds while waiting for ${worker_name} to connect to scheduler"
        exit 2
    fi
    sleep ${worker_poll_interval}
    seconds=$(( ${seconds} + ${worker_poll_interval} ))
done
