/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT WARPING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow WARP_SPOTS {
    take:
    registration_results        // channel:
    spot_extraction_results     // channel:
    outdir                      // file|string - output dir

    main:
    def transform = registration_results
    | map {
        def (
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            transform_output,
            transform_name, transform_subpath,
            inv_transform_output,
            inv_transform_name, inv_transform_subpath
        ) = it

        def id = reg_meta.mov_id
        [ id, inv_transform_output, inv_transform_name, inv_transform_subpath ]
    }

    def spots = spot_extraction_results
    | map {
        def (
            meta,
            image_container,
            image_dataset,
            spots_file
        ) = it

        def id = meta.id
        [ id, spots_file ]
    }

    def spots_warp_input = spots
    | join(transform, by: 0)
    | map {
        def (
            id,
            spots_file,
            inv_transform_output,
            inv_transform_name,
            inv_transform_subpath
        ) = it

        [
            id,
            "${inv_transform_output}/${inv_transform_name}",
            inv_transform_subpath
        ]
    }

    spots_warp_input.subscribe { log.info "!!!!! WARP INPUTS $it " }

    emit:
    done = spots_warp_input
}
