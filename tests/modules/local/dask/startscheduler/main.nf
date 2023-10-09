#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { 
    DASK_STARTSCHEDULER;
} from '../../../../../modules/local/dask/startscheduler/main.nf'

include { 
    DASK_TERMINATE;
} from '../../../../../modules/local/dask/terminate/main.nf'

workflow test_start_dask_scheduler {
    def dask_work_dirname = "${workDir}/test/dask/${workflow.sessionId}"
    def dask_work_dir = file(dask_work_dirname)
    
    if (!dask_work_dir.exists()) dask_work_dir.mkdirs()

    def dask_cluster_input = [
        dask_work_dir
    ]

    DASK_STARTSCHEDULER(dask_cluster_input)
    DASK_TERMINATE(DASK_STARTSCHEDULER.out.clusterpath)
}

workflow test_terminate_before_starting_dask_scheduler {
    def dask_work_dirname = "${workDir}/test/dask/${workflow.sessionId}"
    def dask_work_dir = file(dask_work_dirname)
    
    if (!dask_work_dir.exists()) dask_work_dir.mkdirs()

    def terminateDaskFile = new File("${dask_work_dir}", 'terminate-dask')
    terminateDaskFile.createNewFile() 

    def dask_cluster_input = [
        dask_work_dir
    ]

    DASK_STARTSCHEDULER(dask_cluster_input)
}