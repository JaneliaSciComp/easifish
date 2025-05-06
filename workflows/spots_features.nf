include { SPOTS_REGIONPROPS } from '../modules/local/spots/regionprops/main'
include { SPOTS_COUNTS      } from '../modules/local/spots/counts/main'
include { as_list           } from './util_functions'

workflow SPOTS_STATS {
    take:
    ch_spots        // channel: [ meta_spots, meta_reg, spots_input_image, spots_input_dataset, spots, warped_spots ]
    ch_segmentation // channel: [ meta, seg_input_image, seg_input_dataset, seg_labels ]
    outputdir

    main:
    def ch_segmentation_with_id = ch_segmentation
    | map {
        def (meta, seg_input_image, seg_input_dataset, seg_labels) = it
        def id = meta.id
        def r = [ id, meta, seg_input_image, seg_input_dataset, seg_labels ]
        log.debug "Segmentation data for spots sizes: $r"
        r
    }

    def spots_counts_input = ch_spots
    | filter {
        def (meta_spots, meta_reg, spots_input_image, spots_input_dataset, source_spots, final_spots) = it
        if (!final_spots) {
            log.debug "Filter out spots input for: $it"
            return false
        }
        return true
    }
    | map {
        def (meta_spots, meta_reg, spots_input_image, spots_input_dataset, source_spots, final_spots) = it
        def id = meta_reg.fix_id
        def r = [ id, meta_spots, spots_input_image, spots_input_dataset, source_spots, final_spots ]
        log.debug "Spots data for spots sizes: $r"
        r
    }
    | groupTuple(by: 0)
    | join(ch_segmentation_with_id, by: 0)
    | flatMap {
        def (id,
             all_meta_spots,
             all_spots_image_containers, all_spots_datasets,
             all_source_spots, all_final_spots,
             meta_seg,
             seg_input_image,
             seg_input_dataset,
             seg_labels) = it

        [all_meta_spots,
         all_spots_image_containers,
         all_spots_datasets,
         all_source_spots,
         all_final_spots].transpose()
         .collect {
            def (meta_spots, spots_image_container, spots_dataset, source_spots, final_spots) = it
            def spots_dir = file(final_spots).parent
            def spots_counts_output_dir = file("${outputdir}/${params.spots_counts_subdir}/${meta_spots.id}")
            def adjusted_spots_dataset = sync_image_scale_with_labels_scale(spots_dataset, seg_input_dataset)

            def r = [
                meta_spots,
                spots_image_container, adjusted_spots_dataset,
                seg_labels, seg_input_dataset,
                spots_dir,
                '*coord.csv',
                spots_counts_output_dir,
            ]
            log.debug "Spots sizes input: $r"
            r
         }
    }

    def spots_counts_outputs = SPOTS_COUNTS(
        spots_counts_input,
        params.spots_counts_cores,
        params.spots_counts_mem_gb,
    )

    emit:
    done = spots_counts_outputs.results
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
        // include the registered round id
        // so that it can be used for properly creating the dataset
        [ reg_meta.fix_id, reg_meta.mov_id, warped, warped_subpath ]
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
        // the fixed round id is included
        // to make the result compatible with the one for moving rounds
        [ reg_meta.fix_id, reg_meta.fix_id, fix, fix_subpath ]
    }
    | unique { it[0] } // unique by id

    def ch_segmentation_with_id = ch_segmentation
    | map {
        def (meta, seg_input_image, seg_input_dataset, seg_labels) = it
        def id = meta.id
        def r = [ id, meta, seg_input_image, seg_input_dataset, seg_labels ]
        log.debug "Segmentation data for regionprops: $r"
        r
    }

    def regionprops_inputs = fixed_images
    | concat(registered_images)
    | flatMap {
        def (id, image_id, image_container, image_dataset) = it
        log.debug "Images for regionprops: $it"
        get_spot_subpaths(image_id).collect { subpath, result_name ->
            def r = [
                id,
                image_container,
                subpath,
                result_name,
            ]
            log.debug "Image for regionprops: $r"
            r
        }
    }
    | combine(ch_segmentation_with_id, by: 0)
    | map {
        def (id,
             image_container, image_dataset, result_name,
             meta,
             seg_input_image, seg_input_dataset, seg_labels) = it
        log.debug "Combined cell images with segmentation: $it"

        def regionprops_output_dir = file("${outdir}/${params.spots_props_subdir}/${meta.id}")
        def adjusted_image_dataset = sync_image_scale_with_labels_scale(image_dataset, seg_input_dataset)

        def dapi_dataset = params.dapi_channel
            ? change_dataset_channel(adjusted_image_dataset, params.dapi_channel)
            : ''
        def bleeding_dataset = params.bleeding_channel
            ? change_dataset_channel(adjusted_image_dataset, params.bleeding_channel)
            : ''

        def r = [
            meta,
            image_container, adjusted_image_dataset,
            seg_labels, seg_input_dataset,
            dapi_dataset,
            bleeding_dataset,
            regionprops_output_dir,
            result_name,
        ]
        log.debug "Cell regionprops input: $r"
        r
    }

    SPOTS_REGIONPROPS(
        regionprops_inputs,
        params.spots_props_cores,
        params.spots_props_mem_gb,
    )

    emit:
    done = SPOTS_REGIONPROPS.out.results
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

def get_spot_subpaths(id) {
    if (!params.spot_subpaths && !params.spot_channels && !params.spot_scales) {
        return [
            ['', ''],  // empty subpath, empty resultnane - the input image container contains the array dataset
        ]
    } else if (params.spot_subpaths) {
        // in this case the subpaths parameters must match exactly the container datasets
        return as_list(params.spot_subpaths)
            .collect { subpath ->
                def spot_ch = get_dataset_channel(subpath)
                [
                    "${id}/${subpath}",
                    "${id}-${spot_ch}-props.csv",
                ]
            }
    } else {
        def spot_channels;
        if (params.spot_channels) {
            spot_channels = as_list(params.spot_channels)
            log.debug "Use specified spot channels: $spot_channels"
        } else {
            // all but the last channel which typically is DAPI
            def all_channels = as_list(params.channels)
            if (params.dapi_channel) {
                spot_channels = all_channels.findAll { it != params.dapi_channel }
            } else {
                // automatically consider DAPI the last channel
                // this may throw an exception if the channel list is empty or a singleton
                spot_channels = all_channels[0..-2] // all but the last channel
            }
            log.debug "Spot channels: $spot_channels (all from ${params.channels} except the last one)"
        }
        def spot_scales = as_list(params.spot_scales)

        return [spot_channels, spot_scales].combinations()
            .collect { ch, scale ->
                // when channel and scale is used we also prepend the stitched dataset
                def dataset = "${ch}/${scale}"
                def r = [
                    "${id}/${dataset}",
                    "${id}-${ch}-props.csv",
                ]
                log.debug "Spot dataset: $r"
                r
        }
    }
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
