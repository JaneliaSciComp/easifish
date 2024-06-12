process SPARK_TERMINATE {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    // we need to pass the data_paths because it should contain the spark work dir
    // and that needs to be mounted in the container
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*')

    output:
    tuple val(meta), val(spark), path(data_paths)

    script:
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/terminate.sh "$terminate_file_name"
    """
}

/**
 * Terminate the specified Spark clusters.
 */
workflow SPARK_STOP {
    take:
    ch_meta_spark_and_data_files // channel: [ val(meta), val(spark), path(data_paths) ]
    spark_cluster                // boolean: use a distributed cluster?

    main:
    if (spark_cluster) {
        done = SPARK_TERMINATE(ch_meta_spark_and_data_files)
    } else {
        done = ch_meta_spark_and_data_files
    }
    done.subscribe { log.debug "Terminated spark cluster: $it" }

    emit:
    done
}
