process STITCHING_PREPARE {
    tag "${meta.id}"
    label 'process_single'

    input:
     // files input here is only used for
     // properly mounting the dir when running in a container
    tuple val(meta), path(files)

    output:
    tuple val(meta)

    script:
    """
    umask 0002
    mkdir -p ${meta.stitching_dir}
    mkdir -p ${meta.session_work_dir}
    """
}
