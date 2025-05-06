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
    | map {
        def (
            meta_reg,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            transform_output,
            transform_name, transform_subpath,
            inv_transform_path,
            inv_transform_name, inv_transform_subpath
        ) = it

        def id = meta_reg.mov_id
        def r = [ id, meta_reg, inv_transform_path, inv_transform_name, inv_transform_subpath ]
        log.debug "Registration results considered for warping: $r"
        r
    }

    def registration_fix = registration_results
    | map {
        def (meta_reg) = it
        def id = meta_reg.fix_id
        [ id, meta_reg ]
    }
    | unique { it[0] } // unique by id

    def spots = spot_extraction_results
    | map {
        log.debug "Extracted spot: $it"
        def (
            meta,
            spots_image_container,
            spots_dataset,
            spots_file
        ) = it

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
    | join(spots, by: 0)
    | map {
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
    | filter { it[2] /* spots_file must be defined */}
    | combine(transform, by: 0)
    | map {
        def (
            id,
            meta_spots,
            spots_file,
            spots_image_container, spots_dataset,
            meta_reg,
            inv_transform_path,
            inv_transform_name,
            inv_transform_subpath
        ) = it

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
            [], // affine transform
            [

                "${inv_transform_path}/${inv_transform_name}",
                inv_transform_subpath,
            ],
            [
                '' /* dask scheduler */, [] /* dask config */
            ],
        ]
    }

    spots_warp_input.subscribe { log.debug "Warp spots input: $it " }

    def spots_warp_results
    if (!params.skip_warp_spots) {
        BIGSTREAM_TRANSFORMCOORDS(
            spots_warp_input.map { it[0] },
            spots_warp_input.map { it[1] },
            spots_warp_input.map { it[2] },
            spots_warp_input.map { it[3] },
            spots_warp_input.map { it[4] },
            spots_warp_input.map { it[5] },
            params.warp_spots_cpus,
            params.warp_spots_mem_in_gb,
        )

        spots_warp_results = BIGSTREAM_TRANSFORMCOORDS.out.results

        spots_warp_results.subscribe { log.debug "Bigstream transform coords results: $it " }
    } else {
        // skip warp spots
        spots_warp_results = spots_warp_input.map {
            def (meta, spots_file, warped_spots_output_dir, warped_spots_filename) = it[0]

            def r = [
                meta,
                spots_file,
                "${warped_spots_output_dir}/${warped_spots_filename}",
            ]

            log.debug "Skipping warp spots and return: $r"
        }
    }

    def final_spot_results = fixed_spots.concat(spots_warp_results)
    | map {
        def (meta, source_spots, final_spots) = it
        def r = [
            meta.meta_spots,
            meta.meta_reg,
            source_spots, final_spots,
        ]
        log.debug "All (fixed and warped) spot results: $r"
        r
    }
    | join(spot_extraction_results, by: 0)
    | map {
        def (meta_spots, meta_reg, source_spots, final_spots,
             spots_image_container, spots_dataset) = it
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
