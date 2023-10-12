include { DASK_CLUSTER } from '../../../../subworkflows/local/dask_cluster/main.nf'
include { DASK_TERMINATE } from '../../../../modules/local/dask/terminate/main'

workflow test_start_dask_cluster {
    def test_dir = file("${workDir}/dask/dummy")
    test_dir.mkdirs()

    def dask_cluster_input = [
        [id: 'test_start_dask_cluster'],
        [test_dir],
    ]

    def dask_cluster_info = DASK_CLUSTER(
        Channel.of(dask_cluster_input),
        3, // dask workers
        2, // required workers
        1, // worker cores
        1.5, // worker mem
    )

    dask_cluster_info 
    | map { 
        // only get the first 2 fields from input
        def (meta, cluster_work_dir) = it
        log.info "Started dask cluster: $it"
        [ meta, cluster_work_dir ]
    }
    | DASK_TERMINATE
}

workflow test_start_two_dask_clusters {
    def test_dir = file("${workDir}/dask/dummy")
    test_dir.mkdirs()

    def dask_cluster_input = [
        [
            [
                id: 'test_start_two_dask_clusters_1',
            ],
            [test_dir],
        ],
        [
            [
                id: 'test_start_two_dask_clusters_2',
            ], 
            [/* empty data file list*/],
        ],
    ]

    def dask_cluster_info = DASK_CLUSTER(
        Channel.fromList(dask_cluster_input),
        3, // dask workers
        2, // required workers
        1, // worker cores
        1.5, // worker mem
    )

    dask_cluster_info
    | map { 
        // only get the first 2 fields from input
        def (meta, cluster_work_dir) = it
        log.info "Started dask cluster: $it"
        [ meta, cluster_work_dir ]
    }
    | DASK_TERMINATE
}
