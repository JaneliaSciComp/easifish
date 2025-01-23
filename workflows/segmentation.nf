/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CELLPOSE_SEGMENTATION } from '../subworkflows/local/cellpose_segmentation'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow SEGMENTATION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outdir

    main:
    def session_work_dir = "${params.workdir}/${workflow.sessionId}"

    // get volumes to segment
    def seg_volume = ch_meta
    | filter { meta ->
        meta.id == params.segmentation_id
    }

    seg_volume.subscribe { log.debug "Input image for segmentation: $it" }

    CELLPOSE_SEGMENTATION(
        seg_volume,
        params.models_dir,
        params.log_config,
        params.distributed_cellpose,
        params.dask_config,
        session_work_dir,
        params.cellpose_dask_workers,
        params.cellpose_dask_min_workers,
        params.cellpose_dask_worker_cpus,
        params.cellpose_dask_worker_mem_gb,
        params.cellpose_segmentation_cpus,
        params.cellpose_segmentation_mem_gb
    )
}
