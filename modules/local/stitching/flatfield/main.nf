process STITCHING_FLATFIELD {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2'
    cpus { spark.driver_cpus }
    memory { "${spark.driver_memory}g" }

    input:
    tuple val(meta), path(files, stageAs: '?/*'), val(spark)

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory_gb = spark.executor_memory as int
    def driver_memory_gb = spark.driver_memory as int
    """
    # Remove previous flatfield results because the process will fail if it exists
    rm -r ${meta.stitching_dir}/*flatfield || true
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
        org.janelia.flatfield.FlatfieldCorrection
        ${spark.parallelism}
        ${spark.executor_cpus}
        "${executor_memory_gb}g"
        ${spark.driver_cpus}
        "${driver_memory_gb}g" \
        \${app_args[@]} \
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
