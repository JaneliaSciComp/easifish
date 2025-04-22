include { SPOTS_REGIONPROPS } from '../modules/local/spots/regionprops/main'
include { SPOTS_SIZES       } from '../modules/local/spots/sizes/main'

workflow SPOTS_FEATURES {
    take:
    ch_spots_inputs        // channel: [ meta, input_image, input_dataset, spots, warped_spots, seg_input_image, seg_input_dataset, seg_labels ]

    main:

    def spots_sizes_input = ch_spots_inputs
    | map {
        def (meta,
             image,
             image_dataset,
             spots_file,
             warped_spots_file,
             seg_input_image,
             seg_input_dataset,
             seg_labels) = it
        def spots_input_dir = file(warped_spots_file).parent
        def spots_sizes_output_dir =spots_input_dir
        def r = [
            meta,
            image,
            image_dataset,
            seg_labels,
            seg_input_dataset,
            spots_input_dir,
            'spots-*-coord.csv',
            spots_sizes_output_dir,
        ]
        log "Prepare spots sizes input: $it -> $r"
    }

    def spots_sizes_outputs = SPOTS_SIZES(
        spots_sizes_input,
        params.spots_sizes_cores,
        params.spots_sizes_mem_gb,
    )

    emit:
    done = spots_sizes_outputs.results
}
