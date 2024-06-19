process STITCHING_FLATFIELD {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/stitching-spark:1.11.0'
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(files), val(spark)
    path(darkfield_file, stageAs: 'ff/*')
    path(flatfield_file, stageAs: 'df/*')

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def darkfield_file_arg = darkfield_file ? "--darkfield-file ${darkfield_file}" : ''
    def flatfield_file_arg = flatfield_file ? "--flatfield-file ${flatfield_file}" : ''
    """
    # Remove previous flatfield results because the process will fail if it exists
    rm -r ${meta.stitching_dir}/*flatfield || true
    # Create command line parameters
    declare -a app_args
    for file in ${meta.stitching_dir}/*-n5.json
    do
        app_args+=( -i "\$file" )
    done
    /opt/scripts/runapp.sh "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar org.janelia.flatfield.FlatfieldCorrection \
        ${spark.parallelism} ${spark.worker_cores} "${executor_memory}" ${spark.driver_cores} "${driver_memory}" \
        \${app_args[@]} \
        ${darkfield_file_arg} \
        ${flatfield_file_arg} \
        ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """

    stub:
    """
    # Verify the input exists
    test -f ${meta.stitching_dir}/c0-n5.json
    test -f ${meta.stitching_dir}/c1-n5.json

    # Create the output flatfield for each channel
    mkdir -p ${meta.stitching_dir}/c0-flatfield
    mkdir -p ${meta.stitching_dir}/c1-flatfield

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
