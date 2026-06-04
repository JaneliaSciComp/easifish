#!/bin/bash -ue
# Prepare the Dask cluster work directory.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac

if [[ "${dask_work_dir}" == "" ]]; then
    dwork="dask-\$(date -I)"
    mkdir -p \${dwork}
    cluster_work_dir=\$(\${READLINK_TOOL} -m \${dwork})
else
    cluster_work_dir=\$(\${READLINK_TOOL} -m ${dask_work_dir})
fi
cluster_work_fullpath="\${cluster_work_dir}/${meta.id}"

if [[ ! -d "\${cluster_work_fullpath}" ]]; then
    echo "Creating work directory: \${cluster_work_fullpath}"
    mkdir -p "\${cluster_work_fullpath}"
else
    echo "Cleaning existing work directory: \${cluster_work_fullpath}"
    rm -rf \${cluster_work_fullpath}/* || true
fi

echo "Cluster work dir: \${cluster_work_fullpath}"
