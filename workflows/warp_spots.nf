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

    def all_moving_rounds = transform
    | map { it[0] /* id */ }
    | toList()

    def spots = spot_extraction_results
    | map {
        log.debug "Prepare warp spot inputs: $it"
        def (
            meta,
            image_container,
            image_dataset,
            spots_file
        ) = it

        def id = meta.id
        [
            id,
            meta,
            spots_file,
            image_container, image_dataset,
        ]
    }

    def fixed_spots = spots
    | filter { !(it]0] in all_moving_rounds)  }
    | map {
        def (id, meta, spots_file) = it
        def r = [
            meta,
            spots_file,
            spots,
        ]
        log.debug "Fixed spots: $id -> $r"
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

        spots_warp_results.subscribe { log.debug "Warp spots results: $it " }
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
    def final_spot_results = fixed_spots.mix(spots_warp_results)
    | join(spot_extraction_results, by: 0)
    | map {
        def (meta, spots_file, warped_spots_file, image_container, image_dataset) = it
        def r = [
            meta,
            image_container, image_dataset, // include the image used for spot extraction in the results
            spots_file, warped_spots_file,
        ]
        log.debug "Final spot results: $it -> $r"
        r
    }

    emit:
    done = final_spot_results
}
