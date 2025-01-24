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
    | flatMap { meta ->
        def input_img_dir = "${meta.stitching_result_dir}/${meta.stitching_container}"
        def segmentation_subpaths
        if (params.segmentation_subpath) {
            segmentation_subpaths = as_list(params.segmentation_subpath)
        } else {
            def seg_channels = as_list(params.seg_channels)
            def seg_scales = as_list(params.seg_scales)

            segmentation_subpaths = [seg_channels, seg_scales].combinations().collect { it.join('/') }
        }



        segmentation_subpaths.collect { seg_subpath ->
            def input_img_dataset = "${meta.stitched_dataset}/${seg_subpath}"

            [
                meta,
                input_img_dir,
                input_img_dataset,
                "${outdir}/${params.segmentation_subdir}", // output dir
                params.segmentation_imgname,
            ]
        }
    }

    seg_volume.subscribe { log.debug "Segmentation input: $it" }

    def cellpose_results = CELLPOSE_SEGMENTATION(
        seg_volume,
        params.skip_segmentation,
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

    emit:
    done = cellpose_results
}
