include { SPOTS_PROPS  } from '../modules/local/spots/props/main'

workflow EXTRACT_SPOTS_PROPS {
    take:
    ch_registration
    ch_segmentation
    outdir

    main:
    def registered_images = ch_registration
    | flatMap { it ->
        def (reg_meta,
             fix, fix_subpath,
             mov, mov_subpath,
             warped, warped_subpath,
             transform_output, transform_name, transform_subpath,
             inv_transform_output, inv_transform_name, inv_transform_subpath) = it
        log.debug "Get registered images for region props: $it"
        def join_id = reg_meta.fix_id
        def image_id = reg_meta.mov_id
        def spots_meta = [
            id: reg_meta.mov_id,
            sample_channels: reg_meta.mov_sample_channels,
            spots_channels: reg_meta.mov_spots_channels,
            dapi_channel: reg_meta.mov_dapi_channel,
        ]
        get_spots_features_subpath(spots_meta).collect { subpath, result_name, image_ch ->
            def warped_image_ch
            def bleeding_channel
            def dapi_channel
            if (reg_meta.warped_channels_mapping) {
                // if any of the moving, bleeding or dapi was mapped to a different channel
                // in the registration output, then get the appropriate channel value
                if (image_ch as String in reg_meta.warped_channels_mapping) {
                    // if the image channel was mapped to a different channel
                    warped_image_ch = reg_meta.warped_channels_mapping[image_ch as String]
                } else {
                    warped_image_ch = image_ch
                }
                if (params.bleeding_channel && params.bleeding_channel as String in reg_meta.warped_channels_mapping) {
                    bleeding_channel = reg_meta.warped_channels_mapping[params.bleeding_channel as String]
                } else {
                    bleeding_channel = params.bleeding_channel
                }
                if (params.dapi_channel && params.dapi_channel as String in reg_meta.warped_channels_mapping) {
                    dapi_channel = reg_meta.warped_channels_mapping[params.dapi_channel as String]
                } else {
                    dapi_channel = params.dapi_channel
                }
            } else {
                warped_image_ch = image_ch
                bleeding_channel = params.bleeding_channel
                dapi_channel = params.dapi_channel
            }
            def r = [
                join_id,
                image_id,
                warped,
                warped_subpath,
                warped_image_ch,
                result_name,
                bleeding_channel,
                dapi_channel,
            ]
            log.debug "Registered image for regionprops: $r"
            r
        }
    }

    def fixed_images = ch_registration
    | flatMap { it ->
        def (reg_meta,
             fix, fix_subpath,
             mov, mov_subpath,
             warped, warped_subpath,
             transform_output, transform_name, transform_subpath,
             inv_transform_output, inv_transform_name, inv_transform_subpath) = it
        log.debug "Get fixed image for region props: $it"
        def join_id = reg_meta.fix_id
        def image_id = reg_meta.fix_id
        def spots_meta = [
            id: reg_meta.fix_id,
            sample_channels: reg_meta.fix_sample_channels,
            spots_channels: reg_meta.fix_spots_channels,
            dapi_channel: reg_meta.fix_dapi_channel,
        ]
        get_spots_features_subpath(spots_meta).collect { subpath, result_name, image_ch ->
            def r = [
                join_id,
                image_id,
                fix,
                fix_subpath,
                image_ch,
                result_name,
                params.bleeding_channel,
                params.dapi_channel,
            ]
            log.debug "Fixed image for regionprops: $r"
            r
        }
    }

    def ch_segmentation_with_id = ch_segmentation
    | map { it ->
        def (meta, seg_input_image, seg_input_dataset, seg_labels) = it
        def id = meta.id
        def r = [ id, meta, seg_input_image, seg_input_dataset, seg_labels ]
        log.debug "Segmentation data for regionprops: $r"
        r
    }

    def regionprops_inputs = fixed_images
    | concat(registered_images)
    | unique { it ->
        // get the unique values based join_id, image_id, image_ch
        [it[0], it[1], it[4]]
    } // deduplicate repeated images across registration rounds
    | map { it ->
        log.debug "All images for regionprops: $it"
        it
    }
    | combine(ch_segmentation_with_id, by: 0)
    | map { it ->
        def (_join_id,
             image_id,
             image_container, image_dataset, image_ch,
             result_name, bleeding_channel, dapi_channel,
             meta, seg_input_image, seg_input_dataset, seg_labels) = it
        log.debug "Combined cell images with segmentation: $it"

        def regionprops_output_dir = file("${outdir}/${params.spots_props_subdir}/${image_id}")
        def adjusted_image_dataset = sync_image_scale_with_labels_scale_for_spot_properties(image_dataset, seg_input_dataset)

        log.debug "Synced image and label datasets: ${image_dataset}, ${seg_input_dataset} -> ${adjusted_image_dataset}"

        def dapi_dataset = dapi_channel
            ? change_dataset_channel(adjusted_image_dataset, dapi_channel)
            : ''
        def bleeding_dataset = bleeding_channel
            ? change_dataset_channel(adjusted_image_dataset, bleeding_channel)
            : ''

        log.debug "DAPI dataset: ${dapi_dataset}, bleeding dataset: ${bleeding_dataset}"

        def r = [
            meta,
            image_container, adjusted_image_dataset, image_ch,
            seg_labels, seg_input_dataset,
            dapi_dataset, dapi_channel,
            bleeding_dataset, bleeding_channel,
            regionprops_output_dir,
            result_name,
        ]
        log.debug "Cell regionprops input: $r"
        r
    }

    def spots_props_results
    if (params.skip_spots_properties) {
        spots_props_results = regionprops_inputs
        | map { it ->
            def (meta,
                 image_container, image_dataset, image_ch,
                 seg_labels, seg_input_dataset,
                 dapi_dataset, dapi_ch,
                 bleeding_dataset, bleeding_ch,
                 regionprops_output_dir, result_name) = it
            log.debug "Skip region props: $it"
            [
                meta,
                image_container, image_dataset,
                result_name
            ]
        }
    } else {
        spots_props_results = SPOTS_PROPS(
            regionprops_inputs,
            params.spots_props_cores,
            ParamUtils.get_mem_gb(params.spots_props_mem_gb, params.spots_props_cores, params.default_mem_gb_per_cpu, 0),
        ).results
    }

    emit:
    done = spots_props_results
}


def sync_image_scale_with_labels_scale_for_spot_properties(image_dataset, labels_dataset) {
    def image_dataset_comps = image_dataset.split('/')
    def labels_dataset_comps = labels_dataset.split('/')
    if (labels_dataset_comps && labels_dataset_comps[-1] != image_dataset[-1]) {
        log.debug "Use labels scale for input image: ${labels_dataset_comps[-1]}"
        image_dataset_comps[-1] = labels_dataset_comps[-1]
    }
    return image_dataset_comps.join('/')
}

def get_spots_features_subpath(meta) {
    def spots_channels = SpotsUtils.get_spots_channels(meta, params)
    def spots_subpath = params.spots_subpath ?: ''
    def spots_scale = params.spots_scale ?: ''
    if (spots_channels.empty) {
        return [
        ]
    }
    if (spots_subpath) {
        return spots_channels.collect { ch ->
            [
                "${meta.id}/${spots_subpath}",
                "${meta.id}-${ch}-props.csv",
                "${ch}",
            ]
        }
    } else if (spots_scale) {
        return spots_channels.collect { ch ->
            [
                "${meta.id}/${ch}/${spots_scale}",
                "${meta.id}-${ch}-props.csv",
                "${ch}",
            ]
        }
    } else {
        return []
    }
}

def change_dataset_channel(image_dataset, channel) {
    def image_dataset_comps = image_dataset.split('/')
    if (image_dataset_comps.size() > 2) {
        image_dataset_comps[-2] = channel
    }
    return image_dataset_comps.join('/')
}
