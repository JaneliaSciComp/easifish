process SPARK_STARTMANAGER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*')

    output:
    tuple val(meta), val(spark), path(data_paths)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    spark_local_dir = task.ext.spark_local_dir ?: "/tmp/spark-${workflow.sessionId}"
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_config_filepath = "${spark.work_dir}/spark-defaults.conf"
    spark_master_log_file = "${spark.work_dir}/sparkmaster.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    container_engine = workflow.containerEngine
    """
    full_spark_work_dir=\$(readlink -m ${spark.work_dir})
    if [[ ! -e \${full_spark_work_dir} ]] ; then
        echo "Create spark work directory ${spark.work_dir} -> \${full_spark_work_dir}"
        mkdir -p \${full_spark_work_dir}
    else
        echo "Spark work directory: \${full_spark_work_dir} - already exists"
    fi

    /opt/scripts/startmanager.sh "$spark_local_dir" \${full_spark_work_dir} "$spark_master_log_file" \
        "$spark_config_filepath" "$terminate_file_name" "$args" $sleep_secs $container_engine
    """
}

process SPARK_WAITFORMANAGER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*')

    output:
    tuple val(meta), val(spark), path(data_paths), env(spark_uri)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_master_log_name = "${spark.work_dir}/sparkmaster.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/waitformanager.sh "$spark_master_log_name" "$terminate_file_name" $sleep_secs $max_wait_secs
    export spark_uri=`cat spark_uri`
    """
}

process SPARK_STARTWORKER {
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    cpus { spark.worker_cores }
    memory { spark.worker_memory }

    input:
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*'), val(worker_id)

    output:
    tuple val(meta), val(spark), path(data_paths), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_worker_log_file = "${spark.work_dir}/sparkworker-${worker_id}.log"
    spark_config_filepath = "${spark.work_dir}/spark-defaults.conf"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    worker_memory = spark.worker_memory.replace(" KB",'').replace(" MB",'').replace(" GB",'').replace(" TB",'')
    container_engine = workflow.containerEngine
    """
    /opt/scripts/startworker.sh "${spark.work_dir}" "${spark.uri}" $worker_id \
        ${spark.worker_cores} ${worker_memory} \
        "$spark_worker_log_file" "$spark_config_filepath" "$terminate_file_name" \
        "$args" $sleep_secs $container_engine
    """
}

process SPARK_WAITFORWORKER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    // retry on a timeout to prevent the case when the waiter is started
    // before the worker and the worker never gets its chance
    errorStrategy { task.exitStatus == 2 ? 'retry' : 'terminate' }
    maxRetries 20

    input:
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*'), val(worker_id)

    output:
    tuple val(meta), val(spark), path(data_paths), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_worker_log_file = "${spark.work_dir}/sparkworker-${worker_id}.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/waitforworker.sh "${spark.uri}" \
        "$spark_worker_log_file" "$terminate_file_name" \
        $sleep_secs $max_wait_secs
    """
}

process SPARK_CLEANUP {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    tuple val(meta), val(spark), path(data_paths, stageAs: 'data/?/*'), val(worker_ids)

    output:
    tuple val(meta), val(spark), path(data_paths), val(worker_ids)

    script:
    """
    find ${spark.work_dir} -name app.jar -exec rm {} \\;
    """
}

/**
 * Prepares a Spark context using either a distributed cluster or single process
 * as specified by the `spark_cluster` parameter.
 */
workflow SPARK_START {
    take:
    ch_meta               // channel: [ meta, [data_paths] ]
    spark_cluster         // boolean: use a distributed cluster?
    working_dir           // path: shared storage path for worker communication
    spark_workers         // int: number of workers in the cluster (ignored if spark_cluster is false)
    spark_worker_cpus     // int: number of cores per worker
    spark_gb_per_cpu      // int: number of GB of memory per worker core
    spark_driver_cpus     // int: number of cores for the driver
    spark_driver_mem_gb   // int: number of GB of memory for the driver

    main:
    // create a Spark context for each meta
    def meta_spark_and_data_files = ch_meta
    | map {
        def (meta, data_paths) = it // ch_meta
        def spark_work_dir = file("${working_dir}/spark/${meta.id}")
        def spark = [:]
        spark.work_dir = spark_work_dir
        spark.workers = spark_workers ?: 1
        spark.worker_cores = spark_worker_cpus ?: 1
        spark.driver_cores = spark_driver_cpus ?: 1
        spark.driver_memory = "${spark_driver_mem_gb} GB"
        spark.parallelism = (spark_workers * spark_worker_cpus)
        // 1 GB of overhead for Spark, the rest for executors
        spark.worker_memory = (spark_worker_cpus * spark_gb_per_cpu + 1)+" GB"
        spark.executor_memory = (spark_worker_cpus * spark_gb_per_cpu)+" GB"
        def r = [meta, spark, data_paths + [spark_work_dir]]
        log.debug "Prepare to start spark: $r"
        r
    }

    if (spark_cluster) {
        // start the Spark manager
        // this runs indefinitely until SPARK_TERMINATE is called
        SPARK_STARTMANAGER(meta_spark_and_data_files)

        // start a watcher that waits for the manager to be ready
        SPARK_WAITFORMANAGER(meta_spark_and_data_files)

        SPARK_WAITFORMANAGER.out.subscribe {
            log.debug "Spark manager available: $it"
        }

        // prepare all arguments for all workers
        def meta_workers = SPARK_WAITFORMANAGER.out.flatMap {
            def (meta, spark, data_paths, spark_uri) = it
            spark.uri = spark_uri
            def worker_list = 1..spark.workers
            worker_list.collect { worker_id ->
                [ meta, spark, data_paths, worker_id ]
            }
        }

        meta_workers.subscribe { log.debug "Spark worker input: $it" }

        // start workers
        // these run indefinitely until SPARK_TERMINATE is called
        SPARK_STARTWORKER(meta_workers)

        SPARK_STARTWORKER.out.groupTuple(by:[0,1])
        | map {
            def (meta, spark, data_paths_lists, worker_ids) = it
            [ meta, spark, data_paths_lists.flatten(), worker_ids ]
        }
        | SPARK_CLEANUP // when workers exit they should clean up after themselves

        // wait for all workers to start
        spark_context = SPARK_WAITFORWORKER(meta_workers).groupTuple(by: [0,1])
        | map {
            def (meta, spark, data_paths) = it
            log.debug "Create distributed Spark context: ${meta}, ${spark}"
            // data_paths is a list of list of paths so we need to flatten it
            [ meta, spark, data_paths.flatten() ]
        }
    } else {
        // when running locally, the driver needs enough resources to run a spark worker
        spark_context = meta_spark_and_data_files.map {
            def (meta, spark, data_paths) = it
            spark.workers = 1
            spark.driver_cores = spark_driver_cores + spark_worker_cores
            spark.driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"
            spark.uri = 'local[*]'
            log.debug "Create local Spark context: ${meta}, ${spark}"
            [ meta, spark, data_paths ]
        }
    }

    emit:
    spark_context // channel: [ val(meta), val(spark), path(data_paths) ]
}
