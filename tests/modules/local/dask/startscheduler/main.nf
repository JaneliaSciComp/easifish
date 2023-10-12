#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { 
    DASK_STARTSCHEDULER;
} from '../../../../../modules/local/dask/startscheduler/main'

include { 
    DASK_TERMINATE;
} from '../../../../../modules/local/dask/terminate/main'

workflow test_start_dask_scheduler {
    def dask_work_dirname = "${workDir}/test_start_dask_scheduler/dask/${workflow.sessionId}"
    def dask_work_dir = file(dask_work_dirname)
    
    if (!dask_work_dir.exists()) dask_work_dir.mkdirs()

    def dask_cluster_input = [
        [id:'test_start_dask_scheduler'], dask_work_dir
    ]

    DASK_STARTSCHEDULER(dask_cluster_input)
    DASK_TERMINATE(DASK_STARTSCHEDULER.out.clusterpath)
}

workflow test_terminate_before_starting_dask_scheduler {
    def dask_work_dirname = "${workDir}/test_terminate_before_starting_dask_scheduler/dask/${workflow.sessionId}"
    def dask_work_dir = file(dask_work_dirname)
    
    if (!dask_work_dir.exists()) dask_work_dir.mkdirs()

    def terminateDaskFile = new File("${dask_work_dir}", 'terminate-dask')
    terminateDaskFile.createNewFile() 

    def dask_cluster_input = [
        [id: 'test_terminate_before_starting_dask_scheduler'], dask_work_dir
    ]

    DASK_STARTSCHEDULER(dask_cluster_input)
}
