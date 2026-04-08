process STITCHING_CZI2N5 {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2'
    cpus { spark.driver_cpus }
    memory { "${spark.driver_memory as int}g" }
\
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
    def app_args = "-i ${meta.stitching_dir}/tiles.json -o ${meta.stitching_dir}/tiles.n5"
    """
    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        org.janelia.stitching.ConvertCZITilesToN5Spark
        ${spark.parallelism}
        ${spark.executor_cpus}
        "${executor_memory_gb}g"
        ${spark.driver_cpus}
        "${driver_memory_gb}g"
        ${app_args}
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
    def extra_args = task.ext.args ?: ''
    def executor_memory_gb = spark.executor_memory
    def driver_memory_gb = spark.driver_memory
    def app_args = "-i ${meta.stitching_dir}/tiles.json -o ${meta.stitching_dir}/tiles.n5"
    """
    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        org.janelia.stitching.fake.FakeCZITilesToN5Spark
        ${spark.parallelism}
        ${spark.worker_cpus}
        "${executor_memory_gb}g"
        ${spark.driver_cpus}
        "${driver_memory_gb}g"
        ${app_args}
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
