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
        [ id, reg_meta, inv_transform_output, inv_transform_name, inv_transform_subpath ]
    }

    def registration_fix = registration_results
    | map {
        def (reg_meta) = it
        def id = reg_meta.fix_id
        [ id ]
    }

    def spots = spot_extraction_results
    | map {
        log.debug "Extracted spot: $it"
        def (
            meta,
            image_container,
            image_dataset,
            spots_file
        ) = it

        def id = meta.id
        def r = [
            id,
            meta,
            spots_file,
            image_container, image_dataset,
        ]
        log.debug "Extracted spot candidate: $id: $r"
        r
    }
    | unique { it[0] } // unique by id

    def fixed_spots = spots
    | join(registration_fix, by: 0)
    | map {
        def (id, meta, spots_file) = it
        log.debug "Extract only fixed spots from $it"
        def r = [
            meta,
            spots_file,
            spots_file, // no warping for fixed spots
        ]
        log.debug "Fixed spots: $id: $r"
        r
    }

    def spots_warp_input = spots
    | join(transform, by: 0)
    | map {
        def (
            id,
            meta,
            spots_file,
            image_container, image_dataset,
            reg_meta,
            inv_transform_output,
            inv_transform_name,
            inv_transform_subpath
        ) = it

        def warped_spots_output_dir = file("${outdir}/${params.warped_spots_subdir}/${id}")
        def spots_filename = file(spots_file).name
        [
            [
                meta, spots_file, warped_spots_output_dir, "warped-${spots_filename}",
            ],
            [
                image_container, image_dataset,
            ],
            [
                '' /* resolution */, '' /* downsampling factors */
            ],
            [], // affine transform
            [

                "${inv_transform_output}/${inv_transform_name}",
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
    | join(spot_extraction_results, by: 0)
    | map {
        def (meta, spots_file, warped_spots_file, image_container, image_dataset) = it
        log.debug "add image info to spot results: $it"
        def r = [
            meta,
            image_container, image_dataset, // include the image used for spot extraction in the results
            spots_file, warped_spots_file,
        ]
        log.debug "All (fixed and warped) spot results: $r"
        r
    }

    emit:
    done = final_spot_results // [ meta, image_container, image_dataset, spots_file, warped_spots_file ]
}
