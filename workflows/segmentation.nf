/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CELLPOSE_SEGMENTATION } from '../subworkflows/local/cellpose_segmentation'

include { as_list               } from './util_functions'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SEGMENTATION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow SEGMENTATION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outdir          // file|string - output directory

    main:
    def session_work_dir = "${params.workdir}/${workflow.sessionId}"
    def segmentation_ids = as_list(params.segmentation_ids)

    // get volumes to segment
    def seg_volume = ch_meta
    | filter { meta ->
        meta.id in segmentation_ids
    }
    | map { meta ->
        def input_img_container = "${meta.stitching_result_dir}/${meta.stitching_container}"
        def input_dataset = meta.stitched_dataset
        def segmentation_subpath = params.segmentation_subpath
            ? params.segmentation_subpath
            : "${params.segmentation_ch}/${params.segmentation_scale}"

        [
            input_img_container,
            "${input_dataset}/${segmentation_subpath}", // segmentation dataset
            "${outdir}/${params.segmentation_subdir}", // output dir
            params.segmentation_container,
            "${input_dataset}/${segmentation_subpath}", // segmentation dataset
        ]
    }

    seg_volume.subscribe { log.debug "Segmentation input: $it" }

    CELLPOSE_SEGMENTATION(
        seg_volume,
        params.cellpose_models_dir,
        params.cellpose_log_config,
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
