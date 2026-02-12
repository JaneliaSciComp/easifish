/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT WARPING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { BIGSTREAM_TRANSFORMCOORDS } from '../modules/janelia/bigstream/transformcoords/main'

workflow WARP_SPOTS {
    take:
    registration_results        // channel:
    spot_extraction_results     // channel:
    outdir                      // file|string - output dir

    main:
    def transform = registration_results
    | map { it ->
        def (meta_reg,
            _fix, _fix_subpath,
            _mov, _mov_subpath,
            _warped, _warped_subpath,
            _global_transform, global_inv_transform,
            _deform_transform_output, _deform_transform_name, _deform_transform_subpath,
            inv_deform_transform_path, inv_deform_transform_name, inv_deform_transform_subpath) = it

        def id = meta_reg.mov_id
        def r = [ id, meta_reg,
                  global_inv_transform,
                  inv_deform_transform_path, inv_deform_transform_name, inv_deform_transform_subpath,
                ]
        log.debug "Registration results considered for warping: $r"
        r
    }

    def registration_fix = registration_results
    | map { it ->
        def (meta_reg,
            _fix, _fix_subpath,
            _mov, _mov_subpath,
            _warped, _warped_subpath,
            _global_transform, _global_inv_transform,
            _transform_output, _transform_name, _transform_subpath,
            _inv_transform_path, _inv_transform_name, _inv_transform_subpath) = it
        def id = meta_reg.fix_id
        [ id, meta_reg ]
    }
    | unique { it -> it[0] } // unique by id

    registration_fix.view { it -> log.debug "warp_spots - registration fix id: $it" }

    def spots = spot_extraction_results
    | map { it ->
        log.debug "Extracted spot: $it"
        def (meta, spots_image_container, spots_dataset, spots_file) = it

        def id = meta.id
        def r = [
            id,
            meta,
            spots_file,
            spots_image_container, spots_dataset,
        ]
        log.debug "Extracted spot candidate: $id: $r"
        r
    }

    def fixed_spots = registration_fix
    | combine(spots, by: 0)
    | map { it ->
        def (id, meta_reg, meta_spots, spots_file) = it
        log.debug "Extract only fixed spots from $it"
        def r = [
            [ meta_spots:meta_spots, meta_reg:meta_reg ],
            spots_file,
            spots_file, // no warping for fixed spots
        ]
        log.debug "Fixed spots: $id: $r"
        r
    }

    def spots_warp_input = spots
    | filter { it ->
        /* spots_file must be defined */
        def spots_file = it[2]
        if (!spots_file) {
            log.info "No spots file found in the parameter list: $it"
            return false
        }
        return true
    }
    | combine(transform, by: 0)
    | map { it ->
        def (id, meta_spots, spots_file, spots_image_container, spots_dataset,
            meta_reg,
            global_inv_transform,
            inv_deform_transform_path, inv_deform_transform_name, inv_deform_transform_subpath) = it

        def warped_spots_output_dir = file("${outdir}/${params.warped_spots_subdir}/${id}")
        def meta = [ meta_spots:meta_spots, meta_reg:meta_reg ]
        def spots_filename = file(spots_file).name
        [
            [
                meta, spots_file, warped_spots_output_dir, "warped-${spots_filename}",
            ],
            [
                spots_image_container, spots_dataset,
            ],
            [
                '' /* resolution */, '' /* downsampling factors */
            ],
            global_inv_transform ? file(global_inv_transform) : [], // inverse global affine transform
            [
                "${inv_deform_transform_path}/${inv_deform_transform_name}",
                inv_deform_transform_subpath,
            ],
            [
                '' /* dask scheduler */, [] /* dask config */
            ],
        ]
    }

    spots_warp_input.view { it -> log.debug "Warp spots input: $it " }

    def spots_warp_results
    if (!params.skip_warp_spots) {
        BIGSTREAM_TRANSFORMCOORDS(
            spots_warp_input.map { it -> it[0] },
            spots_warp_input.map { it -> it[1] },
            spots_warp_input.map { it -> it[2] },
            spots_warp_input.map { it -> it[3] },
            spots_warp_input.map { it -> it[4] },
            spots_warp_input.map { it -> it[5] },
            params.warp_spots_cpus,
            params.warp_spots_mem_in_gb,
        )

        spots_warp_results = BIGSTREAM_TRANSFORMCOORDS.out.results

        spots_warp_results.view { it -> log.debug "Bigstream transform coords results: $it " }
    } else {
        // skip warp spots
        spots_warp_results = spots_warp_input.map { it ->
            log.debug "Check warp spots input: $it"

            def (meta, spots_file, warped_spots_output_dir, warped_spots_filename) = it[0]

            def r = [
                meta,
                spots_file,
                "${warped_spots_output_dir}/${warped_spots_filename}",
            ]

            log.debug "Skip warp spots return: $r"
            r
        }
    }

    def final_spot_results = fixed_spots
    | concat(spots_warp_results)
    | map { it ->
        def (meta, source_spots, final_spots) = it
        def r = [
            meta.meta_spots,
            meta.meta_reg,
            source_spots, final_spots,
        ]
        log.debug "Prepare (fixed and warped) spot results: $r"
        r
    }
    | join(spot_extraction_results, by: 0)
    | map { it ->
        def (meta_spots, meta_reg, source_spots, final_spots, spots_image_container, spots_dataset) = it
        log.debug "Add source spots image to spot results: $it"
        def r = [
            meta_spots,
            meta_reg,
            spots_image_container, spots_dataset, // include the image used for spot extraction in the results
            source_spots, final_spots,
        ]
        log.debug "All (fixed and warped) spot results: $r"
        r
    }

    emit:
    done = final_spot_results // [ meta_spots, meta_reg, image_container, image_dataset, spots_file, warped_spots_file ]
}
