process STITCHING_STITCH {
    tag "${meta.id}"
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2' }
    cpus { spark.driver_cores }
    memory { "${spark.driver_memory}g" }

    input:
    tuple val(meta), path(files, stageAs: '?/*'), val(spark)
    path(darkfield_file, stageAs: 'df/*')
    path(flatfield_file, stageAs: 'ff/*')

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory_gb = spark.executor_memory as int
    def driver_memory_gb = spark.driver_memory as int
    def darkfield_file_arg = darkfield_file ? "--darkfield-file ${darkfield_file}" : ''
    def flatfield_file_arg = flatfield_file ? "--flatfield-file ${flatfield_file}" : ''
    """
    # Create command line parameters
    declare -a app_args
    for file in ${meta.stitching_dir}/*-n5.json
    do
        app_args+=( -i "\$file" )
    done

    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        org.janelia.stitching.StitchingSpark
        ${spark.parallelism}
        ${spark.executor_cores}
        "${executor_memory_gb}g"
        ${spark.driver_cores}
        "${driver_memory_gb}g"
        --stitch
        \${app_args[@]}
        ${darkfield_file_arg}
        ${flatfield_file_arg}
        ${extra_args}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """

    stub:
    """
    # Create the final output metadata file for each channel
    touch ${meta.stitching_dir}/c0-n5-final.json
    touch ${meta.stitching_dir}/c1-n5-final.json

    # Create optimizer output
    touch ${meta.stitching_dir}/optimizer-final.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
