process STITCHING_STITCH {
    tag "${meta.id}"
    container 'docker.io/janeliascicomp/stitching-spark:1.11.0'
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
    """
    # Create command line parameters
    declare -a app_args
    for file in ${meta.stitching_dir}/*-n5.json
    do
        app_args+=( -i "\$file" )
    done
    echo "Stitching args: \${app_args[@]}"
    /opt/scripts/runapp.sh "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar org.janelia.stitching.StitchingSpark \
        ${spark.parallelism} ${spark.worker_cores} "${executor_memory}" ${spark.driver_cores} "${driver_memory}" \
        --stitch \${app_args[@]} ${extra_args}

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
