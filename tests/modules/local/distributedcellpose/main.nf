include { START_DASK          } from '../../../../subworkflows/local/start_dask/main'
include { DISTRIBUTEDCELLPOSE } from '../../../../modules/local/distributedcellpose/main'
include { STOP_DASK           } from '../../../../subworkflows/local/stop_dask/main'

workflow test_distributed_cellpose_with_dask {
    def test_input_output = [
        file('../multifish-testdata/LHA3_R3_small/stitching/export.n5'),
        file('../multifish-testdata/LHA3_R3_small/segmentation'),
    ]
    def cellpose_test_data = [
        [
            id: 'test_distributed_cellpose_with_dask',
            image_dataset: params.image_dataset,
        ],
        params.dask_config 
            ? test_input_output + [ file(params.dask_config).parent ]
            : test_input_output
    ]
    def cellpose_test_data_ch = Channel.of(cellpose_test_data)
    
    def cluster_info = START_DASK(
        cellpose_test_data_ch,
        true,
        params.cellpose_workers,
        params.cellpose_required_workers,
        params.cellpose_worker_cpus,
        params.cellpose_worker_mem_gb,
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
        params.cellpose_driver_cpus,
        params.cellpose_driver_mem_gb,
    )

    cellpose_results.subscribe {
        log.info "Cellpose results: $it"
    }

    cluster_info.join(cellpose_results, by:0)
    | map {
        def (meta, cluster_context) = it
        [ meta, cluster_context ]
    }
    | STOP_DASK

}