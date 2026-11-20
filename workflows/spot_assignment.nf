include { SPOTS_COUNTS } from '../modules/local/spots/counts/main'

/*
SPOT_COUNT_ASSIGN - assign and spots to a cell (label) and count them
*/
workflow SPOT_COUNT_ASSIGN {
    take:
    ch_spots        // channel: [ meta_spots, meta_reg, spots_input_image, spots_input_dataset, spots, warped_spots ]
    ch_segmentation // channel: [ meta, seg_input_image, seg_input_dataset, seg_labels ]
    outputdir

    main:
    // spots are already in the fixed space (fix round spots or warped spots)
    // so they are paired with the segmentation labels regardless of the registration pair
    def spots_counts_input = ch_spots
    | filter { it ->
        def (_meta_spots, _meta_reg, _spots_input_image, _spots_input_dataset, _source_spots, final_spots) = it
        if (!final_spots) {
            log.debug "Filter out spots input for: $it"
            return false
        }
        return true
    }
    | map { it ->
        def (meta_spots, _meta_reg, spots_input_image, spots_input_dataset, _source_spots, final_spots) = it
        def r = [ meta_spots, spots_input_image, spots_input_dataset, final_spots ]
        log.debug "Spots data for spots counts: $it -> $r"
        r
    }
    | combine(ch_segmentation)
    | map { it ->
        def (meta_spots, spots_image_container, spots_dataset, final_spots,
             _meta_seg, _seg_input_image, seg_input_dataset, seg_labels) = it

        log.debug "Collected info for spots inputs: $it"

        def spots_dir = file(final_spots).parent
        def spots_counts_output_dir = file("${outputdir}/${params.spots_counts_subdir}/${meta_spots.id}")
        def adjusted_spots_dataset = sync_image_scale_with_labels_scale_for_spot_assign(spots_dataset, seg_input_dataset)

        def r = [
            meta_spots,
            spots_image_container, adjusted_spots_dataset,
            seg_labels, seg_input_dataset,
            spots_dir,
            params.spots_counts_pattern,
            spots_counts_output_dir,
        ]
        log.debug "Spots counts input before removing duplicates: $r"
        r
    }
    | unique { it -> it[0].id }

    spots_counts_input.view { it -> log.debug "Spots counts input: $it" }

    def spots_counts_outputs
    if (ParamUtils.as_bool(params.run_spots_counts)) {
        spots_counts_outputs = SPOTS_COUNTS(
            spots_counts_input,
            params.spots_counts_cores,
            ParamUtils.get_mem_gb(params.spots_counts_mem_gb, params.spots_counts_cores, params.default_mem_gb_per_cpu, 0),
        ).results
    } else {
        spots_counts_outputs = spots_counts_input
        | map { it ->
            def (meta_spots, spots_image_container, adjusted_spots_dataset, _seg_labels, _seg_input_dataset,spots_dir, _spots_pattern, spots_counts_output_dir) = it

            log.debug "Skip spots counts $it"
            [
                meta_spots,
                spots_image_container, adjusted_spots_dataset,
                spots_dir,
                spots_counts_output_dir,
            ]
        }
    }

    emit:
    done = spots_counts_outputs
}


def sync_image_scale_with_labels_scale_for_spot_assign(image_dataset, labels_dataset) {
    def image_dataset_comps = image_dataset.split('/')
    def labels_dataset_comps = labels_dataset.split('/')
    if (labels_dataset_comps && labels_dataset_comps[-1] != image_dataset[-1]) {
        log.debug "Use labels scale for input image: ${labels_dataset_comps[-1]}"
        image_dataset_comps[-1] = labels_dataset_comps[-1]
    }
    return image_dataset_comps.join('/')
}
