#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { 
    DASK_STARTSCHEDULER as DASK_STARTSCHEDULER_SIMPLE;
    DASK_STARTSCHEDULER as DASK_STARTSCHEDULER_AFTER_TERMINATE_TRIGGERED;
} from '../../../../modules/local/dask/startscheduler/main.nf'

workflow test_dask_scheduler {

}

workflow test_terminate_dask_scheduler_beforestarted {

}
