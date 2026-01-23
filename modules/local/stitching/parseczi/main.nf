process STITCHING_PARSECZI {
    tag "${meta.id}"
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2' }
    cpus { spark.driver_cores }
    memory { "${spark.driver_memory}g" }

    input:
    tuple val(meta), path(files), val(spark)

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory_gb = spark.executor_memory as int
    def driver_memory_gb = spark.driver_memory as int
    // Find the MVL metadata file
    def mvl = files.findAll { it.extension=="mvl" }.first()
    // Get the CZI filename pattern
    def pattern = meta.pattern
    // If there is no pattern, it must be a single CZI file
    if (pattern==null || pattern=='') {
        def czis = files.findAll { it.extension == 'czi' }
        pattern = czis.first()
    }
    """
    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        org.janelia.stitching.ParseCZITilesMetadata
        ${spark.parallelism}
        ${spark.executor_cores}
        "${executor_memory_gb}g"
        ${spark.driver_cores}
        "${driver_memory_gb}g"
        -i ${mvl}
        -b ${meta.image_dir}
        -f ${pattern}
        -o ${meta.stitching_dir}
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
}
