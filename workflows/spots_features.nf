include { SPOTS_REGIONPROPS } from '../modules/local/spots/regionprops/main'
include { SPOTS_SIZES       } from '../modules/local/spots/sizes/main'

workflow SPOTS_FEATURES {
    take:
    ch_spots_inputs        // channel: [ meta, input_image, input_dataset, spots, warped_spots, seg_input_image, seg_input_dataset, seg_labels ]
    outputdir

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
        log.debug "Prepare spots sizes input from $it"

        def spots_input_dir = file(warped_spots_file).parent
        def spots_sizes_output_dir = file("${outputdir}/${params.spots_features_subdir}/${meta.id}")

        def image_dataset_comps = image_dataset.split('/')
        def labels_dataset_comps = seg_input_dataset.split('/')
        if (labels_dataset_comps && labels_dataset_comps[-1] != image_dataset_comps[-1]) {
            log.debug "Use labels scale for input image: ${labels_dataset_comps[-1]}"
            image_dataset_comps[-1] = labels_dataset_comps[-1]
        }

        def r = [
            meta,
            image,
            image_dataset_comps.join('/'),
            seg_labels,
            seg_input_dataset,
            spots_input_dir,
            '*coord.csv',
            spots_sizes_output_dir,
        ]
        log.debug "Spots sizes input: $r"
        r
    }

    def spots_sizes_outputs = SPOTS_SIZES(
        spots_sizes_input,
        params.spots_sizes_cores,
        params.spots_sizes_mem_gb,
    )

    emit:
    done = spots_sizes_outputs.results
}
