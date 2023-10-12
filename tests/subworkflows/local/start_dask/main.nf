include { START_DASK } from '../../../../subworkflows/local/start_dask/main.nf'
include { DASK_TERMINATE } from '../../../../modules/local/dask/terminate/main'

workflow test_local_dask {
    def dask_cluster_input = [
        [id: 'test_start_dask_cluster'],
        [/* empty data paths */],
    ]

    def dask_cluster_info = START_DASK(
        Channel.of(dask_cluster_input),
        false,
        3, // dask workers
        2, // required workers
        1, // worker cores
        1.5, // worker mem
    )

    dask_cluster_info 
    | filter { meta, data, dask_context ->
        log.info "Dask context for ${meta}: ${dask_context}"
        return dask_context.cluster_work_dir
    }
    | DASK_TERMINATE
}

workflow test_distributed_dask {
    def dask_cluster_input = [
        [id: 'test_start_dask_cluster'],
        [/* empty data paths */],
    ]

    def dask_cluster_info = START_DASK(
        Channel.of(dask_cluster_input),
        true,
        3, // dask workers
        2, // required workers
        1, // worker cores
        1.5, // worker mem
    )

    dask_cluster_info 
    | filter { meta, data, dask_context ->
        log.info "Dask context for ${meta}: ${dask_context}"
        dask_context.cluster_work_dir
    }
    | map { meta, data, dask_context ->
        [ meta, dask_context.cluster_work_dir ]
    }
    | DASK_TERMINATE
}
