include { START_DASK          } from '../../../../subworkflows/local/start_dask/main'
include { DISTRIBUTEDCELLPOSE } from '../../../../modules/local/distributedcellpose/main'
include { STOP_DASK           } from '../../../../subworkflows/local/stop_dask/main'

workflow test_distributed_cellpose_with_dask {
    def cellpose_test_data = [
        [
            id: 'test_distributed_cellpose_with_dask',
            image_dataset: 'c1/s3',
        ],
        [
            file('../multifish-testdata/LHA3_R3_small/stitching/export.n5'),
            file('../multifish-testdata/LHA3_R3_small/segmentation'),
        ]
    ]
    def cellpose_test_data_ch = Channel.of(cellpose_test_data)
    def cellpose_workers = 2
    def cellpose_required_workers = 2
    def cellpose_driver_cpus = 1
    def cellpose_driver_mem_gb = 6
    def cellpose_worker_cpus = 1
    def cellpose_worker_mem_gb = 3
    
    def cluster_info = START_DASK(
        cellpose_test_data_ch,
        true,
        cellpose_workers,
        cellpose_required_workers,
        cellpose_worker_cpus,
        cellpose_worker_mem_gb,
    )

    def cellpose_input = cluster_info
    | join(cellpose_test_data_ch, by: 0)
    | multiMap { meta, cluster_context, datapaths ->
        data: [ meta, datapaths[0], datapaths[1] ]
        cluster: cluster_context.scheduler_address
    }

    cellpose_input.data.subscribe {
        log.info "Cellpose data: $it"
    }

    cellpose_input.cluster.subscribe {
        log.info "Cellpose cluster: $it"
    }

    def cellpose_results = DISTRIBUTEDCELLPOSE(
        cellpose_input.data,
        cellpose_input.cluster,
        cellpose_driver_cpus,
        cellpose_driver_mem_gb,
    )

    cluster_info.join(cellpose_results, by:0)
    | map {
        def (meta, cluster_context) = it
        [ meta, cluster_context ]
    }
    | STOP_DASK

}