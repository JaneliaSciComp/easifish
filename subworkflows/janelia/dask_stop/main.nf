process DASK_TERMINATE {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/dask:2024.12.1-py11-ol9'

    input:
    tuple val(meta), path(cluster_work_dir, stageAs: 'dask_work/*')

    output:
    tuple val(meta), env('cluster_work_fullpath')

    when:
    task.ext.when == null || task.ext.when

    script:
    def cluster_work_path = cluster_work_dir
    def terminate_file_name = "${cluster_work_path}/terminate-dask"
    """
    case \$(uname) in
        Darwin)
            detected_os=OSX
            READLINK_TOOL="greadlink"
            ;;
        *)
            detected_os=Linux
            READLINK_TOOL="readlink"
            ;;
    esac
    echo "Detected OS: \${detected_os}"
    cluster_work_fullpath=\$(\${READLINK_TOOL} ${cluster_work_dir})

    echo "\$(date): Terminate DASK Scheduler: ${cluster_work_path}"
    echo \$PWD
    cat > ${terminate_file_name} <<EOF
    \$(date)
    DONE
    EOF

    cat ${terminate_file_name}
    """
}

workflow DASK_STOP {
    take:
    meta_and_context     // channel: [val(meta), dask_context]

    main:
    def cluster_info = meta_and_context
    | filter { _meta, dask_context ->
        // only terminate the clusters that have a work dir
        dask_context.cluster_work_dir
    }
    | map { meta, dask_context ->
        log.debug "Stop Dask ${meta}: ${dask_context}"
        [ meta, dask_context.cluster_work_dir ]
    }
    | DASK_TERMINATE

    emit:
    done = cluster_info // [ meta, dask_work_dir ]
}
