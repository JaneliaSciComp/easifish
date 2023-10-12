include { DASK_PREPARE } from '../../../modules/local/dask/prepare/main'
include { DASK_STARTSCHEDULER } from '../../../modules/local/dask/startscheduler/main'
include { DASK_CLUSTERINFO } from '../../../modules/local/dask/clusterinfo/main'
include { DASK_STARTWORKER } from '../../../modules/local/dask/startworker/main'
include { DASK_CHECKWORKERS } from '../../../modules/local/dask/checkworkers/main'

workflow DASK_CLUSTER {
    take:
    meta_and_files       // channel: [val(meta), files...]
    dask_workers         // int: number of total workers in the cluster
    required_workers     // int: number of required workers in the cluster
    dask_worker_cores    // int: number of cores per worker
    dask_worker_mem_db   // int: worker memory in GB

    main:
    def dask_prepare_result = meta_and_files.map { meta, data ->
        def cluster_work_dir = file("${workDir}/dask/${meta.id}")
        log.debug "Prepare cluster work dir for $meta, $data -> $cluster_work_dir"
        [
            meta, cluster_work_dir.parent.parent, cluster_work_dir,
        ]
    }
    | DASK_PREPARE // prepare dask work dir -> [ meta, cluster_work_dir ]
    
    // start scheduler
    DASK_STARTSCHEDULER(dask_prepare_result)

    // get cluster info
    DASK_CLUSTERINFO(dask_prepare_result)

    def dask_cluster_info = DASK_CLUSTERINFO.out.cluster_info

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

    dask_cluster_info.subscribe {
        log.info "!!!! CLUSTER INFO: $it"
    }
    // check dask workers
    DASK_CHECKWORKERS(dask_cluster_info, dask_workers, required_workers)

    emit:
    done = DASK_CHECKWORKERS.out.cluster_info // [ meta, cluster_work_dir, scheduler_address ]
}
