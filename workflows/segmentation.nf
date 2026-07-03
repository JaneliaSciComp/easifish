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
    outdir          // file|string - output directory

    main:
    def session_work_dir = "${params.workdir}/${workflow.sessionId}"
    log.info "Session work directory: ${session_work_dir}"
    def segmentation_ids = ParamUtils.as_list(params.segmentation_ids)

    if (segmentation_ids.empty) {
        log.info "No segmentation ids were set"
    }
    // get volumes to segment
    // typically this is done for the DAPI channel of the fixed round
    def seg_volume = ch_meta
    | filter { meta ->
        params.segmentation_input || meta.id in segmentation_ids
    }
    | flatMap { meta ->
        def input_img_dir
        def segmentation_subpaths
        if (params.segmentation_input) {
            input_img_dir = file(params.segmentation_input)
        } else {
            input_img_dir = "${meta.stitching_result_dir}/${meta.stitching_container}"
        }

        if (params.segmentation_subpaths) {
            // the subpaths parameters must match exactly the container datasets
            segmentation_subpaths = ParamUtils.as_list(params.segmentation_subpaths)
                .collect { subpath ->
                     "${meta.stitched_dataset}/${subpath}"
                }
        } else {
            segmentation_subpaths = [ '' ] // empty subpath - the input image container contains the array data
        }

        def mask_file
        if (!params.cellpose_mask) {
            mask_file = []
        } else if (params.cellpose_mask.startsWith('/')) {
            // this is an absolute mask
            mask_file = file(params.cellpose_mask)
        } else {
            // use a path relative to outdir
            mask_file = file("${outdir}/${params.cellpose_mask}")
        }
        def mask_sp = params.cellpose_mask_subpath
                        ? (params.cellpose_mask_subpath.startsWith(meta.id)
                            ? params.cellpose_mask_subpath
                            : "${meta.id}/${params.cellpose_mask_subpath}")
                        : ''
        segmentation_subpaths.collect { input_seg_subpath ->
            def r = [
                meta,
                input_img_dir,
                input_seg_subpath,
                mask_file,
                mask_sp,
                "${outdir}/${params.segmentation_subdir}", // output dir
                params.segmentation_imgname,
            ]
            log.debug "Segmentation input: $r"
            r
        }
    }

    def cellpose_dask_worker_mem_gb = ParamUtils.get_mem_gb(
        params.cellpose_dask_worker_mem_gb,
        params.cellpose_dask_worker_cpus,
        params.default_mem_gb_per_cpu,
        0)
    def cellpose_results = CELLPOSE_SEGMENTATION(
        seg_volume,
        ParamUtils.as_bool(params.run_segmentation),
        ParamUtils.as_bool(params.run_standalone_merge_labels),
        params.cellpose_models_dir,
        params.cellpose_model,
        params.cellpose_preprocessing_config_file,
        params.cellpose_log_config,
        ParamUtils.as_bool(params.run_segmentation_multiscale),
        ParamUtils.as_bool(params.distributed_cellpose),
        params.dask_config,
        session_work_dir,
        params.cellpose_dask_workers,
        params.cellpose_dask_min_workers,
        params.cellpose_dask_worker_cpus,
        cellpose_dask_worker_mem_gb,
    )

    emit:
    done = cellpose_results // [ meta, input_image, input_dataset, output_segmentation_file ]
}
