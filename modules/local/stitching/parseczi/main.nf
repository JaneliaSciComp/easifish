process STITCHING_PARSECZI {
    tag "${meta.id}"
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0' }
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(files), val(spark)

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    extra_args = task.ext.args ?: ''
    executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    // Find the MVL metadata file
    mvl = files.findAll { it.extension=="mvl" }.first()
    // Get the CZI filename pattern
    pattern = meta.pattern
    // If there is no pattern, it must be a single CZI file
    if (pattern==null || pattern=='') {
        czis = files.findAll { it.extension=="czi" }
        pattern = czis.first()
    }
    """
    /opt/scripts/runapp.sh "$workflow.containerEngine" "$spark.work_dir" "$spark.uri" \
        /app/app.jar org.janelia.stitching.ParseCZITilesMetadata \
        ${spark.parallelism} ${spark.worker_cores} "${executor_memory}" ${spark.driver_cores} "${driver_memory}" \
        -i ${mvl} -b ${meta.image_dir} -f ${pattern} -o ${meta.stitching_dir} ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
