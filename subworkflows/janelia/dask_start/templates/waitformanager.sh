#!/bin/bash -ue
# Wait for the Dask scheduler info file to appear, then extract the address/port.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
cluster_work_fullpath=\$(\${READLINK_TOOL} ${cluster_work_dir})
scheduler_info_file="\${cluster_work_fullpath}/dask-scheduler-info.json"
terminate_file_name="\${cluster_work_fullpath}/terminate-dask"

${moduleDir}/templates/waitforanyfile.sh 0 "\${scheduler_info_file},\${terminate_file_name}" ${task.ext.scheduler_start_timeout ?: '60'} ${task.ext.scheduler_poll_interval ?: '2'}

if [[ -e "\${scheduler_info_file}" ]]; then
    echo "\$(date): Get cluster info from \${scheduler_info_file}"
    scheduler_address=\$(jq ".address" \${scheduler_info_file})
    dashboard_port=\$(jq ".services.dashboard" \${scheduler_info_file})
else
    echo "\$(date): Cluster info file \${scheduler_info_file} not found"
    scheduler_address=
    dashboard_port=
fi

dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//")
cat <<-END_VERSIONS > versions.yml
"dask": \${dask_version}
END_VERSIONS
