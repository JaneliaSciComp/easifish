include { DASK_PREPARE        } from '../../../modules/local/dask/prepare/main'
include { DASK_STARTMANAGER   } from '../../../modules/local/dask/startmanager/main'
include { DASK_WAITFORMANAGER } from '../../../modules/local/dask/waitformanager/main'
include { DASK_STARTWORKER    } from '../../../modules/local/dask/startworker/main'
include { DASK_WAITFORWORKERS } from '../../../modules/local/dask/waitforworkers/main'

workflow DASK_CLUSTER {
    take:
    meta_and_files       // channel: [val(meta), files...]
    dask_workers         // int: number of total workers in the cluster
    required_workers     // int: number of required workers in the cluster
    dask_worker_cores    // int: number of cores per worker
    dask_worker_mem_db   // int: worker memory in GB

    main:
    def dask_prepare_result = meta_and_files.map { meta, data ->
        def dask_work_dir = params.dask_work_dir ?: "${workDir}/dask"
        def cluster_work_dir = file(dask_work_dir).resolve(meta.id)
        log.debug "Cluster work dir for ${meta}, ${data} -> ${dask_work_dir}, ${cluster_work_dir}"
        [
            meta, file(dask_work_dir), cluster_work_dir,
        ]
    }
    | DASK_PREPARE // prepare dask work dir -> [ meta, cluster_work_dir ]
    
    // start scheduler
    DASK_STARTMANAGER(dask_prepare_result)

    // wait for manager to start
    DASK_WAITFORMANAGER(dask_prepare_result)

    def dask_cluster_info = DASK_WAITFORMANAGER.out.cluster_info

    // prepare inputs for dask workers
    def workers_list = 1..dask_workers

    def dask_workers_input = dask_cluster_info
    | join(meta_and_files, by: 0)
    | combine(workers_list)
    | multiMap { meta, cluster_work_dir, scheduler_address, data, worker_id ->
        worker_info: [ meta, cluster_work_dir, scheduler_address, worker_id ]
        data: data
    }

    // start dask workers
    DASK_STARTWORKER(dask_workers_input.worker_info,
                     dask_workers_input.data,
                     dask_worker_cores,
                     dask_worker_mem_db)

    // check dask workers
    def cluster = DASK_WAITFORWORKERS(dask_cluster_info, dask_workers, required_workers)

    cluster.cluster_info.subscribe {
        log.debug "Cluster info: $it"
    }

    emit:
    done = cluster.cluster_info // [ meta, cluster_work_dir, scheduler_address, available_workers ]
}
