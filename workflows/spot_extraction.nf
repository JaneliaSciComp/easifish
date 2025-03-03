/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT EXTRACTION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { RS_FISH } from '../modules/janelia/rs_fish/main'
include { as_list } from './util_functions'

workflow SPOT_EXTRACTION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outdir          // file|string - output directory

    main:
    def spot_volume_ids = as_list(params.spot_extraction_ids)

    def spots_input_volume = ch_meta
    | filter { meta ->
        spot_volume_ids.empty || meta.id in spot_volume_ids
    } flatMap { meta ->
        def input_img_dir = "${meta.stitching_result_dir}/${meta.stitching_container}"
        def spot_subpaths
        if (!params.spot_subpaths && !params.spot_channels && !params.spot_scales) {
            spot_subpaths = [ '' ] // empty subpath - the input image container contains the array data
        } else if (params.spot_subpaths) {
            // in this case the subpaths parameters must match exactly the container datasets
            spot_subpaths = as_list(params.spot_subpaths)
        } else {
            def spot_channels = as_list(params.spot_channels)
            def spot_scales = as_list(params.spot_scales)

            spot_subpaths = [spot_channels, spot_scales].combinations()
                .collect {
                    // when channel and scale is used we also prepend the stitched dataset
                    def dataset = it.join('/')
                    "${meta.stitched_dataset}/${dataset}"
            }
        }

        spot_subpaths.collect { input_spot_subpath ->
            [
                meta,
                input_img_dir,
                input_spot_subpath,
                "${outdir}/${params.spot_extraction_subdir}", // output dir
                params.spot_extraction_imgname,
            ]
        }
    }

    spots_input_volume.subscribe { log.debug "Spot extraction input volume: $it" }

    emit:
    done = spots_input_volume
}
