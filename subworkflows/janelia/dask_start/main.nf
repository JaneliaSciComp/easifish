
workflow DASK_START {
    take:
    meta_and_files // channel: [val(meta), files...]
    distributed // bool: if true create distributed cluster
    dask_config // dask config
    dask_work_path // dask work directory
    total_workers // int: number of total workers in the cluster
    required_workers // int: number of required workers in the cluster
    dask_worker_cpus // int: number of cores per worker
    dask_worker_mem_gb // int: worker memory in GB

    main:
    if (distributed) {
        // prepare dask cluster work dir meta -> [ meta, cluster_work_dir ]
        def dask_work_dir
        if (dask_work_path) {
            dask_work_dir = file(dask_work_path)
            def dask_work_dirname = dask_work_dir.name
            if (dask_work_dirname != "${workflow.sessionId}") {
                dask_work_dir = file("${dask_work_path}/${workflow.sessionId}")
            }
        } else {
            dask_work_dir = []
        }
        def dask_prepare_result = DASK_PREPARE(
            meta_and_files,
            dask_work_dir,
        )
        | join(meta_and_files, by: 0)
        | map { it ->
            def (meta, dask_cluster_work_dir, data_paths) = it
            def dask_config_path = dask_config ? file(dask_config) : []
            def r = [meta, dask_config_path, dask_cluster_work_dir, data_paths]
            log.debug("Dask prepare result: ${r}")
            r
        }

        // start scheduler
        DASK_STARTMANAGER(dask_prepare_result)

        // wait for manager to start
        DASK_WAITFORMANAGER(
            dask_prepare_result.map { it ->
                def (meta, _dask_config_path, dask_cluster_work_dir, _data_paths) = it
                [meta, dask_cluster_work_dir]
            }
        )

        def nworkers = total_workers ?: 1

        // prepare inputs for dask workers
        def dask_workers_input = DASK_WAITFORMANAGER.out.cluster_info
            | join(meta_and_files, by: 0)
            | flatMap { it ->
                def (meta, cluster_work_dir, scheduler_address, dashboard_port, data_paths) = it
                def dashboard_address_parts = scheduler_address.split(':')
                dashboard_address_parts[0] = 'http'
                dashboard_address_parts[-1] = dashboard_port

                log.info "Scheduler address: ${scheduler_address} -> ${dashboard_address_parts.join(':')}"
                def dask_config_path = dask_config ? file(dask_config) : []
                def worker_list = 1..nworkers
                worker_list.collect { worker_id ->
                    def r = [meta, dask_config_path, cluster_work_dir, scheduler_address, worker_id, data_paths]
                    log.debug("Dask workers input: ${r}")
                    r
                }
            }

        // start dask workers
        DASK_STARTWORKER(
            dask_workers_input,
            dask_worker_cpus,
            dask_worker_mem_gb,
        )

        // check dask workers
        def cluster = DASK_WAITFORWORKERS(
            DASK_WAITFORMANAGER.out.cluster_info,
            nworkers,
            required_workers ?: 1,
        )

        dask_context = cluster.cluster_info
            | map { it ->
                def (meta, cluster_work_dir, scheduler_address, available_workers) = it
                def dask_info = [
                    scheduler_address: scheduler_address,
                    cluster_work_dir: cluster_work_dir,
                    available_workers: available_workers,
                ]
                log.debug("Cluster info: ${it} -> [ ${meta}, ${dask_info} ] ")
                [meta, dask_info]
            }
    }
    else {
        // do not start a distributed cluster
        log.debug("No distributed dask cluster")
        dask_context = meta_and_files
            | map { it ->
                def (meta, _data_paths) = it
                [meta, [:]]
            }
    }

    emit:
    dask_context // [ meta, dask_info ]
}

process DASK_PREPARE {
    tag "${meta.id}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/dask:2025.5.1-py12-ol9' }

    input:
    tuple val(meta), path(data, stageAs: '?/*')
    path dask_work_dir, stageAs: 'dask_work/*'

    output:
    tuple val(meta), env('cluster_work_fullpath')

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    if [[ "${dask_work_dir}" == "" ]]; then
        dwork="dask-\$(date -I)"
        mkdir -p \${dwork}
        cluster_work_dir=\$(readlink -m \${dwork})
    else
        cluster_work_dir=\$(readlink -m ${dask_work_dir})
    fi
    cluster_work_fullpath="\${cluster_work_dir}/${meta.id}"
    /opt/scripts/daskscripts/prepare.sh "\${cluster_work_fullpath}"
    echo "Cluster work dir: \${cluster_work_fullpath}"
    """
}

process DASK_STARTMANAGER {
    tag "${meta.id}"
    label 'process_long'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/dask:2025.5.1-py12-ol9' }

    input:
    tuple val(meta), path(dask_config), path(cluster_work_dir, stageAs: 'dask_work/*'), path(data, stageAs: '?/*')

    output:
    tuple val(meta), env('cluster_work_fullpath'), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def container_engine = workflow.containerEngine

    def set_dask_config_env = dask_config ? "export DASK_CONFIG=\$(readlink ${dask_config})" : ''
    def dask_scheduler_pid_file = "${cluster_work_dir}/dask-scheduler.pid"
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    """
    cluster_work_fullpath=\$(readlink ${cluster_work_dir})
    ${set_dask_config_env}

    echo "Scheduler's environment"
    env

    /opt/scripts/daskscripts/startmanager.sh \
        --container-engine ${container_engine} \
        --pid-file ${dask_scheduler_pid_file} \
        --scheduler-work-dir ${cluster_work_dir} \
        --scheduler-file ${dask_scheduler_info_file} \
        --terminate-file ${terminate_file_name} \
        ${args}

    dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//" )
    cat <<-END_VERSIONS > versions.yml
    "dask": \${dask_version}
    END_VERSIONS
    """
}

process DASK_STARTWORKER {
    tag "${meta.id}:${worker_id}"
    label 'process_long'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/dask:2025.5.1-py12-ol9' }
    cpus { worker_cpus }
    memory "${worker_mem_in_gb} GB"

    input:
    tuple val(meta), path(dask_config), path(cluster_work_dir, stageAs: 'dask_work/*'), val(scheduler_address), val(worker_id), path(data, stageAs: '?/*')
    val worker_cpus
    val worker_mem_in_gb

    output:
    tuple val(meta), env('cluster_work_fullpath'), val(scheduler_address), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def container_engine = workflow.containerEngine

    def set_dask_config_env = dask_config ? "export DASK_CONFIG=\$(readlink ${dask_config})" : ''
    def dask_worker_name = "worker-${worker_id}"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_worker_work_dir = "${cluster_work_dir}/${dask_worker_name}"
    def dask_worker_pid_file = "${dask_worker_work_dir}/${dask_worker_name}.pid"

    """
    cluster_work_fullpath=\$(readlink ${cluster_work_dir})

    ${set_dask_config_env}
    echo "Worker's environment"
    env

    /opt/scripts/daskscripts/startworker.sh \
        --container-engine ${container_engine} \
        --name ${dask_worker_name} \
        --worker-dir ${dask_worker_work_dir} \
        --scheduler-address ${scheduler_address} \
        --pid-file ${dask_worker_pid_file} \
        --memory-limit "${worker_mem_in_gb}G" \
        --terminate-file ${terminate_file_name} \
        ${args}

    dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//" )
    cat <<-END_VERSIONS > versions.yml
    "dask": \${dask_version}
    END_VERSIONS
    """
}

process DASK_WAITFORMANAGER {
    tag "${meta.id}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/dask:2025.5.1-py12-ol9' }

    input:
    tuple val(meta), path(cluster_work_dir, stageAs: 'dask_work/*')

    output:
    tuple val(meta),
          env('cluster_work_fullpath'),
          env('scheduler_address'),
          env('dashboard_port'), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    """
    cluster_work_fullpath=\$(readlink ${cluster_work_dir})

    /opt/scripts/daskscripts/waitformanager.sh \
        --flist "${dask_scheduler_info_file},${terminate_file_name}" \
        ${args}

    if [[ -e "${dask_scheduler_info_file}" ]] ; then
        echo "\$(date): Get cluster info from ${dask_scheduler_info_file}"
        scheduler_address=\$(jq ".address" ${dask_scheduler_info_file})
        dashboard_port=\$(jq ".services.dashboard" ${dask_scheduler_info_file})
    else
        echo "\$(date): Cluster info file ${dask_scheduler_info_file} not found"
        scheduler_address=
        dashboard_port=
    fi

    dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//" )
    cat <<-END_VERSIONS > versions.yml
    "dask": \${dask_version}
    END_VERSIONS
    """
}

process DASK_WAITFORWORKERS {
    tag "${meta.id}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/dask:2025.5.1-py12-ol9' }

    input:
    tuple val(meta), path(cluster_work_dir, stageAs: 'dask_work/*'), val(scheduler_address), val(dashboard_port)
    val total_workers
    val required_workers

    output:
    tuple val(meta), env('cluster_work_fullpath'), val(scheduler_address), env('available_workers'), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def container_engine = workflow.containerEngine

    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()

    """
    cluster_work_fullpath=\$(readlink ${cluster_work_dir})

    # waitforworkers.sh sets available_workers variable
    . /opt/scripts/daskscripts/waitforworkers.sh \
        --cluster-work-dir ${cluster_work_dir} \
        --scheduler-address ${scheduler_address} \
        --total-workers ${total_workers} \
        --required-workers ${required_workers} \
        --terminate-file ${terminate_file_name} \
        ${args}

    dask_version=\$(dask --version | grep version | sed "s/.*version\\s*//" )
    cat <<-END_VERSIONS > versions.yml
    "dask": \${dask_version}
    END_VERSIONS
    """
}
