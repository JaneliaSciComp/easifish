include { DASK_STARTCLUSTER } from '../../../../subworkflows/local/dask_startcluster/main.nf'
include { DASK_TERMINATE } from '../../../../modules/local/dask/terminate/main'

workflow test_start_dask_cluster {
    def dask_work_dirname = "${workDir}/test_start_dask_cluster/dask/${workflow.sessionId}"
    def dask_work_dir = file(dask_work_dirname)

    if (!dask_work_dir.exists()) dask_work_dir.mkdirs()

    def dask_cluster_input = [
        [dask_work_dir: dask_work_dir], 'adummyfile', 'anotherdummyfile'
    ]
    def dask_cluster_info = DASK_STARTCLUSTER(
        Channel.of(dask_cluster_input),
        3, // dask workers
        2, // required workers
        1, // worker cores
        1.5, // worker mem
    )

    dask_cluster_info | view

    dask_cluster_info 
    | map { 
        it[1] // cluster_work_dir
    }
    | DASK_TERMINATE
}
