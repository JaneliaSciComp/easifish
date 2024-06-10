process STITCHING_PREPARE {
    tag "${meta.id}"
    label 'process_single'

    input:
    tuple val(meta), path(files)

    output:
    tuple val(meta), val(files)

    script:
    """
    umask 0002
    mkdir -p ${meta.stitching_dir}
    mkdir -p ${meta.session_work_dir}
    """
}
