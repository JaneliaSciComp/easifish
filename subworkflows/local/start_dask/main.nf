include { DASK_CLUSTER } from '../dask_cluster/main'

workflow START_DASK {
    take:
    meta_and_files       // channel: [val(meta), files...]
    distributed_cluster  // bool: if true create distributed cluster
    dask_workers         // int: number of total workers in the cluster
    required_workers     // int: number of required workers in the cluster
    dask_worker_cores    // int: number of cores per worker
    dask_worker_mem_db   // int: worker memory in GB

    main:
    def cluster_info
    if (distributed_cluster) {
        log.info "!!!!!!! DISTRIBUTED DASK: $distributed_cluster"
        cluster_info = DASK_CLUSTER(meta_and_files, dask_workers, required_workers, dask_worker_cores, dask_worker_mem_db)
        | join(meta_and_files, by: 0)
        | map { meta, cluster_work_dir, scheduler_address, available_workers, data ->
            dask_context = [
                scheduler_address: scheduler_address,
                cluster_work_dir: cluster_work_dir,
                available_workers: available_workers,
            ]
            [ meta, data, dask_context ]
        }
    } else {
        log.info "!!!!!!! LOCAL DASK: $distributed_cluster"
        cluster_info = meta_and_files
        | map { meta, data ->
            [ meta, data, [:] ]
        }
    }

    emit:
    done = cluster_info // [ meta, cluster_work_dir, scheduler_address, available_workers ]
}
