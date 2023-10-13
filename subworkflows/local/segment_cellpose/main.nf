include { START_DASK          } from 'start_dask/main'
include { DISTRIBUTEDCELLPOSE } from 'distributedcellpose/main'
include { STOP_DASK           } from 'stop_dask/main'

workflow SEGMENT_CELLPOSE {

    take:
    ch_input                   // [ meta, image ]
    distribute_cluster         // bool: if true distribute cellpose segmentation
    cellpose_driver_cpus       // cpus needed by cellpose main
    cellpose_driver_mem_gb     // mem needed by cellpose main
    cellpose_workers           // number of cellpose workers
    cellpose_required_workers  // number of required cellpose workers
    cellpose_worker_cpus       // cpus needed by cellpose workers      
    cellpose_worker_mem_gb     // mem needed by cellpose workers

    main:
    def cluster_info = START_DASK(
        ch_input,
        distribute_cellpose,
        cellpose_workers,
        cellpose_required_workers,
        cellpose_worker_cores,
        cellpose_worker_mem_gb,
    )
    
    def cellpose_input = cluster_info
    | join(ch_input, by: 0)
    | multiMap { meta, cluster_info, image ->
        image: [ meta, cluster_work_dir, scheduler_address, worker_id ]
        cluster: cluster_info.scheduler_address
    }

    DISTRIBUTEDCELLPOSE(
        cellpose_input.image,
        cellpose_input.cluster,
        cellpose_driver_cpus
        cellpose_driver_mem_gb
    )

}