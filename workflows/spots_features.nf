include { SPOTS_REGIONPROPS } from '../modules/local/spots/regionprops/main'
include { SPOTS_SIZES       } from '../modules/local/spots/sizes/main'

workflow MEASURE_SPOTS {
    take:
    ch_spots        // channel: [ meta_spots, meta_reg, spots_input_image, spots_input_dataset, spots, warped_spots ]
    ch_segmentation // channel: [ meta, seg_input_image, seg_input_dataset, seg_labels ]
    outputdir

    main:
    def ch_segmentation_with_id = ch_segmentation
    | map {
        def (meta, seg_input_image, seg_input_dataset, seg_labels) = it
        def id = meta.id
        [ id, meta, seg_input_image, seg_input_dataset, seg_labels ]
    }

    def spots_sizes_input = ch_spots
    | map {
        def (meta_spots, meta_reg, spots_input_image, spots_input_dataset, source_spots, final_spots) = it
        def id = meta_reg.fix_id
        [ id, meta_spots, spots_input_image, spots_input_dataset, source_spots, final_spots ]
    }
    | combine(ch_segmentation, by: 0)
    | map {
        def (id,
             meta_spots,
             spots_image_container, spots_dataset,
             source_spots, final_spots,
             meta_seg, seg_input_image, seg_input_dataset, seg_labels) = it
        log.debug "Combined spots and segmentation results: $it"

        def spots_dir = file(warped_spots).parent
        def spots_sizes_output_dir = file("${outputdir}/${params.spots_sizes_subdir}/${meta_spots.id}")

        adjusted_spots_dataset = sync_image_scale_with_labels_scale(spots_dataset, seg_input_dataset)

        def r = [
            meta_spots,
            spots_image_container, adjusted_spots_dataset,
            seg_labels, seg_input_dataset,
            spots_dir,
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

workflow EXTRACT_CELL_REGIONPROPS {
    take:
    ch_registration
    ch_segmentation
    outdir

    main:
    def registered_images = ch_registration
    | map {
        def (reg_meta,
             fix, fix_subpath,
             mov, mov_subpath,
             warped, warped_subpath,
             transform_output,
             transform_name, transform_subpath,
             inv_transform_output,
             inv_transform_name, inv_transform_subpath) = it
        [ reg_meta.fix_id, warped, warped_subpath ]
    }

    def fixed_images = ch_registration
    | map {
        def (reg_meta,
             fix, fix_subpath,
             mov, mov_subpath,
             warped, warped_subpath,
             transform_output,
             transform_name, transform_subpath,
             inv_transform_output,
             inv_transform_name, inv_transform_subpath) = it
        [ reg_meta.fix_id, fix, fix_subpath ]
    }
    | unique { it[0] } // unique by id

    def ch_segmentation_with_id = ch_segmentation
    | map {
        def (meta, seg_input_image, seg_input_dataset, seg_labels) = it
        def id = meta.id
        [ id, meta, seg_input_image, seg_input_dataset, seg_labels ]
    }

    def regionprops_inputs = fixed_images
    | concat(registered_images)
    | combine(ch_segmentation_with_id, by: 0)
    | map {
        def (id,
             image_container, image_dataset,
             meta,
             seg_input_image, seg_input_dataset, seg_labels) = it
        log.debug "Combined cell images with segmentation: $it"

        def regionprops_output_dir = file("${outdir}/${params.cells_regionprops_subdir}/${meta.id}")
        def adjusted_image_dataset = sync_image_scale_with_labels_scale(image_dataset, seg_input_dataset)

        def dapi_dataset = params.dapi_channel
            ? change_dataset_channel(adjusted_image_dataset, params.dapi_channel)
            : ''
        def bleeding_dataset = params.bleeding_channel
            ? change_dataset_channel(adjusted_image_dataset, params.bleeding_channel)
            : ''

        def dataset_ch = get_dataset_channel(adjusted_image_dataset)

        def r = [
            meta,
            image_container, adjusted_image_dataset,
            seg_labels, seg_input_dataset,
            dapi_dataset,
            bleeding_dataset,
            regionprops_output_dir,
            "${meta.id}-${dataset_ch}-regionprops.csv",
        ]
        log.debug "Cell regionprops input: $r"
        r
    }

    emit:
    done = regionprops_inputs

}


def sync_image_scale_with_labels_scale(image_dataset, labels_dataset) {
    def image_dataset_comps = image_dataset.split('/')
    def labels_dataset_comps = labels_dataset.split('/')
    if (labels_dataset_comps && labels_dataset_comps[-1] != image_dataset[-1]) {
        log.debug "Use labels scale for input image: ${labels_dataset_comps[-1]}"
        image_dataset_comps[-1] = labels_dataset_comps[-1]
    }
    return image_dataset_comps.join('/')
}

def change_dataset_channel(image_dataset, channel) {
    def image_dataset_comps = image_dataset.split('/')
    if (image_dataset_comps) {
        image_dataset_comps[-2] = channel
    }
    return image_dataset_comps.join('/')
}

def get_dataset_channel(image_dataset) {
    def image_dataset_comps = image_dataset.split('/')
    return image_dataset_comps ? image_dataset_comps[-2] : ''
}
