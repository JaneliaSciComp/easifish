include { DASK_PREPARE } from '../../../modules/local/dask/prepare/main'
include { DASK_STARTSCHEDULER as DASK_STARTSCHEDULER_PROCESS } from '../../../modules/local/dask/startscheduler/main'
include { DASK_CLUSTERINFO } from '../../../modules/local/dask/clusterinfo/main'
include { DASK_STARTWORKER } from '../../../modules/local/dask/startworker/main'
include { DASK_CHECKWORKERS } from '../../../modules/local/dask/checkworkers/main'

workflow DASK_STARTCLUSTER {
    take:
    meta_and_files       // channel: [val(meta), files...]
    dask_workers         // int: number of total workers in the cluster
    required_workers     // int: number of required workers in the cluster
    dask_worker_cores    // int: number of cores per worker
    dask_worker_mem_db   // int: worker memory in GB

    main:
    def dask_work_dir = meta_and_files.map { 
        it[0].dask_work_dir 
    }

    def dask_prepare_input = dask_work_dir.map { 
        def r = [
            file(it).parent,
            file(it).name
        ]
        r
    }

    // prepare dask work dir
    DASK_PREPARE(dask_prepare_input)

    // start the scheduler
    DASK_STARTSCHEDULER_PROCESS(DASK_PREPARE.out)

    // get cluster info
    DASK_CLUSTERINFO(DASK_PREPARE.out)

    def dask_cluster_info = DASK_CLUSTERINFO.out.cluster_info | map {
        log.debug "dask_cluster_info: $it"
        def (cluster_scheduler_address, cluster_workdir) = it
        [ cluster_scheduler_address, cluster_workdir ]
    }

    def workers_list = 1..dask_workers

    def dask_workers_inputs = dask_cluster_info.combine(workers_list)

    dask_workers_inputs.subscribe {
        log.debug "dask_workers_inputs: ${it}"
    }

    DASK_STARTWORKER(dask_workers_inputs, dask_worker_cores, dask_worker_mem_db)
   
    DASK_CHECKWORKERS(dask_cluster_info, dask_workers, required_workers)

    emit:
    done = DASK_CHECKWORKERS.out.cluster_info
}
