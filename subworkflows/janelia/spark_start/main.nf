process PREPARE_SPARK_CONFIG {
    tag "${meta.id}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir), path(spark_local_dir)
    val(spark_config)

    output:
    tuple val(meta), val(spark), env('full_spark_work_dir'), emit: spark_inputs
    env('spark_config_filepath')                           , emit: spark_config_file

    script:
    if (spark.executor_cpus > 0) {
        spark_config['spark.executor.cores'] = spark.executor_cpus
    }
    if (spark.executor_memory > 0) {
        spark_config['spark.executor.memory'] = spark.executor_memory instanceof Integer
            ? "${spark.executor_memory}g"
            : "${spark.executor_memory * 1024 as int}m"
    }
    if (spark.executor_memory_overhead > 0) {
        spark_config['spark.executor.memoryOverhead'] = spark.executor_memory_overhead instanceof Integer
            ? "${spark.executor_memory_overhead}g"
            : "${spark.executor_memory_overhead * 1024 as int}m"
    }
    spark_config['spark.task.cpus'] = "${spark.task_cpus}"
    def spark_config_content = spark_config.inject('# Spark configuration') { acc, k, v ->
        "${acc}\n${k}=${v}"
    }
    """
    # Prepare the Spark working directory and write spark-defaults.conf.

    case \$(uname) in
        Darwin) READLINK_TOOL="greadlink" ;;
        *)      READLINK_TOOL="readlink"  ;;
    esac
    full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
    spark_local_tmp_dir="${spark_local_dir ? spark_local_dir : '/tmp'}"
    full_spark_local_dir="\$(\${READLINK_TOOL} -m \${spark_local_tmp_dir})/spark-${workflow.sessionId}"
    spark_config_filepath="\${full_spark_work_dir}/spark-defaults.conf"

    if [[ ! -e \${full_spark_work_dir} ]]; then
        echo "Create spark work directory ${spark_work_dir} -> \${full_spark_work_dir}"
        mkdir -p \${full_spark_work_dir}
    else
        echo "Spark work directory: \${full_spark_work_dir} - already exists"
    fi

    echo "Create spark config file \${spark_config_filepath}"
    echo "${spark_config_content}" > \${spark_config_filepath}
    echo "spark.local.dir=\${full_spark_local_dir}" >> \${spark_config_filepath}
    """
}

process SPARK_STARTMANAGER {
    tag "${meta.id}"
    label 'process_long'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir), path(spark_local_dir)

    output:
    tuple val(meta), val(spark), env('full_spark_work_dir')

    when:
    task.ext.when == null || task.ext.when

    script:
    template 'startmanager.sh'
}

process SPARK_WAITFORMANAGER {
    tag "${meta.id}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate'
    }
    maxRetries 20

    input:
    tuple val(meta), val(spark), path(spark_work_dir)

    output:
    tuple val(meta), val(spark), env('full_spark_work_dir'), env('spark_uri')

    when:
    task.ext.when == null || task.ext.when

    script:
    template 'waitformanager.sh'
}

process SPARK_STARTWORKER {
    tag "${meta.id}:${worker_id}"
    label 'process_long'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'
    cpus { spark.worker_cpus }
    memory { "${spark.worker_memory}g" }

    input:
    tuple val(meta), val(spark), path(spark_work_dir), path(spark_local_dir), val(worker_id)
    path(data_paths, stageAs: '?/*')

    output:
    tuple val(meta), val(spark), env('full_spark_work_dir'), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    template 'startworker.sh'
}

process SPARK_WAITFORWORKERS {
    tag "${meta.id}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir)
    val total_workers
    val required_workers

    output:
    tuple val(meta), val(spark), env('full_spark_work_dir'), env('available_workers')

    when:
    task.ext.when == null || task.ext.when

    script:
    template 'waitforworkers.sh'
}

process SPARK_CLEANUP {
    tag "${meta.id}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir)

    output:
    tuple val(meta), val(spark)

    script:
    template 'cleanup.sh'
}

process SPARK_RUNAPP {
    tag "${meta.id}"
    label 'process_long'
    container 'ghcr.io/janeliascicomp/spark:4.1.2-scala2.13-java21-ubuntu24.04'
    cpus   { spark.driver_cpus }
    memory { "${spark.driver_memory}g" }

    input:
    tuple val(meta), val(spark), path(spark_work_dir), path(app_jar_path), val(default_app_jar), val(app_spark_conf), val(app_main_class), val(app_args)
    path(data_files, stageAs: "?/*") // this is passed with the intention of mounting data files inside the container

    output:
    tuple val(meta), val(spark)

    when:
    task.ext.when == null || task.ext.when

    script:
    app_jar = "${app_jar_path ?: default_app_jar ?: '/app/app.jar'}"
    app_spark_conf_args = app_spark_conf ? app_spark_conf.collect { k, v -> "--conf '${k}=${v}'" }.join(' ') : ''
    executor_memory_gb = spark.executor_memory as int
    driver_memory_gb = spark.driver_memory as int
    log.debug "Run spark app using jar file: ${app_jar}, spark config: ${app_spark_conf_args}, executor memory: ${executor_memory_gb}, driver memory: ${driver_memory_gb}"
    template 'runapp.sh'
}

/**
 * Prepares a Spark context using either a distributed cluster or single process
 * as specified by the `spark_cluster` parameter.
 */
workflow SPARK_START {
    take:
    ch_meta                         // channel: [ meta, [data_paths] ]
    config                          // map: spark configuration options
    spark_cluster                   // boolean: use a distributed cluster?
    working_dir                     // path: shared storage path for worker communication
    local_dir                       // path: spark local executor directory
    spark_workers                   // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_workers                     // int: number of minimum required workers
    spark_worker_cpus               // int: number of cpus per worker
    spark_worker_mem_gb             // int: number of GB of memory  per worker
    spark_executor_cpus             // int: number of cpus per executor
    spark_executor_mem_gb           // int: number of GB of memory per executor
    spark_executor_overhead_mem_gb  // int: executor memory overhead in GB
    spark_task_cpus                 // int: number of cpus allocated to a task - this is used for determining the task parallelism
    spark_driver_cpus               // int: number of cpus for the driver
    spark_driver_mem_gb             // int: number of GB of memory for the driver
    spark_gb_per_cpu                // int: number of GB of memory per worker core

    main:
    // create a Spark context for each meta
    def n_spark_workers = spark_workers ?: 1

    def spark_config = config + [
        'spark.rpc.askTimeout': '300s',
        'spark.storage.blockManagerHeartBeatMs': '30000',
        'spark.rpc.retry.wait': '30s',
        'spark.kryoserializer.buffer.max': '1024m',
        'spark.core.connection.ack.wait.timeout': '600s',
        'spark.driver.maxResultSize': '0',
        'spark.worker.cleanup.enabled': 'true',
    ]

    def spark_inputs = ch_meta
        .map { it ->
            def (meta, _data_paths) = it // ch_meta: [ meta, [data_paths] ] - ignore data_paths
            def spark_work_dir = file("${working_dir}/spark/${meta.id}")
            def spark_local_dir = local_dir ? file(local_dir) : []

            def worker_cpus = spark_worker_cpus ?: 1
            def executor_cpus = spark_executor_cpus ?: 1
            def driver_cpus = spark_driver_cpus ?: 1
            def task_cpus = spark_task_cpus ?: 1
            def executors_per_worker = (worker_cpus / executor_cpus) as int
            def tasks_per_executor = (executor_cpus / task_cpus) as int
            def parallelism = n_spark_workers * executors_per_worker * tasks_per_executor
            def executor_memory_overhead = spark_executor_overhead_mem_gb ?: 0
            def worker_memory = spark_worker_mem_gb ?: (worker_cpus * spark_gb_per_cpu)
            def executor_memory = spark_executor_mem_gb ?: (executor_cpus * spark_gb_per_cpu - executor_memory_overhead)
            def driver_memory = spark_driver_mem_gb ?: (spark_driver_cpus * spark_gb_per_cpu)

            def spark = [
                work_dir: spark_work_dir,
                local_dir: spark_local_dir,
                workers: n_spark_workers,
                worker_cpus: worker_cpus,
                executor_cpus: executor_cpus,
                task_cpus: task_cpus,
                driver_cpus: driver_cpus,
                worker_memory: worker_memory,
                executor_memory: executor_memory,
                driver_memory: driver_memory,
                executor_memory_overhead: executor_memory_overhead,
                parallelism: parallelism,
            ]
            def r = [
                meta,
                spark,
                spark_work_dir,
                spark_local_dir,
            ]
            log.debug "Prepare to start spark: $r"
            r
        }

    def prepare_config_results = PREPARE_SPARK_CONFIG(spark_inputs, spark_config)

    if (Boolean.valueOf(spark_cluster)) {
        // start the Spark manager
        // this runs indefinitely until SPARK_TERMINATE is called
        SPARK_STARTMANAGER(
            prepare_config_results.spark_inputs
                .map { meta, spark, spark_work_dir -> [meta, spark, spark_work_dir, spark.local_dir] }
        )

        // start a watcher that waits for the manager to be ready
        // SPARK_WAITFORMANAGER only needs spark_work_dir (no spark_local_dir)
        def wait_manager_results = SPARK_WAITFORMANAGER(prepare_config_results.spark_inputs)

        def spark_workers_common_input = wait_manager_results
            .join(ch_meta, by:0) // join with ch_meta to get the data files in order to mount them in the workers
            .map { it ->
                def (meta, spark, spark_work_dir, spark_uri, data_paths) = it
                log.debug "Spark manager available: ${meta}: ${spark} using spark work dir: ${spark_work_dir}"
                // Copy into a new map instead of mutating the shared instance in place.
                // The same `spark` map is retained in task contexts, and Nextflow's async
                // cache writer may Kryo-serialize it concurrently - an in-place put races
                // with that iteration and throws ConcurrentModificationException.
                def spark_with_uri = spark + [uri: spark_uri]
                [ meta, spark_with_uri, spark_work_dir, data_paths ]
            }

        // prepare all arguments for each worker
        def meta_workers_with_data = spark_workers_common_input
            .flatMap { it ->
                def (meta, spark, spark_work_dir, data_paths) = it
                def worker_list = 1..spark.workers
                worker_list.collect { worker_id ->
                    [ meta, spark, spark_work_dir, spark.local_dir, worker_id, data_paths ]
                }
            }
            .multiMap { it ->
                def (meta, spark, spark_work_dir, spark_local_dir, worker_id, data_paths) = it
                log.debug "Spark worker input: $it"
                start_worker: [ meta, spark, spark_work_dir, spark_local_dir, worker_id ]
                data: data_paths
            }

        // start workers
        // these run indefinitely until SPARK_TERMINATE is called
        def spark_workers_results = SPARK_STARTWORKER(
            meta_workers_with_data.start_worker,
            meta_workers_with_data.data,
        )

        // SPARK_STARTWORKER output: [ meta, spark, full_spark_work_dir, worker_id ]
        def spark_cleanup_input = spark_workers_results
        .groupTuple(by:[0,1,2], size: n_spark_workers)
        .map { it ->
            def (meta, spark, spark_work_dir, _worker_ids) = it
            [ meta, spark, spark_work_dir ]
        }

        SPARK_CLEANUP(spark_cleanup_input) // when workers exit they should clean up after themselves

        // wait for all workers to start
        def needed_workers = min_workers <= 0 || min_workers > n_spark_workers
                                ? n_spark_workers
                                : min_workers
        spark_context = SPARK_WAITFORWORKERS(
            spark_workers_common_input.map { it -> it[0..-2] /* do not include the data_paths */ },
            n_spark_workers,
            needed_workers
        )
        .map { it ->
            def (meta, spark, _spark_work_dirs, available_workers) = it
                log.debug "Create distributed Spark context: ${meta}, ${spark} with ${available_workers} workers"
                [ meta, spark ]
        }
    } else {
        log.debug "Use local spark"
        // when running locally, the driver needs enough resources to run a spark worker
        spark_context = prepare_config_results.spark_inputs.map { it ->
            def (meta, spark, _spark_work_dir) = it

            // Copy into a new map instead of mutating the shared instance in place
            // (see the cluster path above - avoids a ConcurrentModificationException
            // when Nextflow's cache writer serializes the map concurrently).
            def local_spark = spark + [
                workers      : 1,
                driver_cpus  : spark.driver_cpus + spark.worker_cpus,
                driver_memory: spark.driver_memory + spark.executor_memory,
                uri          : 'local[*]',
            ]
            log.debug "Create local Spark context: ${meta}, ${local_spark}"
            [ meta, local_spark ]
        }
    }

    emit:
    spark_context // channel: [ val(meta), val(spark) ]
}
