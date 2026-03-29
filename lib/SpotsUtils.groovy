import nextflow.Nextflow

class SpotsUtils {

    static def get_spots_subpaths(meta, params) {
        def sample_channels
        if (meta.sample_channels) {
            sample_channels = ParamUtils.as_list(meta.sample_channels)
            Nextflow.log.debug "Sample channels for ${meta.id}: ${sample_channels}"
        } else {
            sample_channels = ParamUtils.as_list(params.channels)
            Nextflow.log.debug "All channels ${sample_channels}"
        }
        def dapi_channel = meta.dapi_channel ?: params.dapi_channel

        def spots_channels
        if (meta.spots_channels) {
            spots_channels = ParamUtils.as_list(meta.spots_channels)
            Nextflow.log.debug "Spot channels for ${meta.id}: ${spots_channels}"
        } else if (params.spots_channels) {
            spots_channels = ParamUtils.as_list(params.spots_channels)
            Nextflow.log.debug "Spot channels param: ${spots_channels}"
        } else {
            if (dapi_channel) {
                spots_channels = sample_channels.findAll { sc -> sc != dapi_channel }
                Nextflow.log.debug "Spot channels from sample channels ${sample_channels} - DAPI channel ${dapi_channel} -> ${spots_channels}"
            } else {
                // automatically consider DAPI the last channel
                // so return all but the last channel
                // this may throw an exception if the channel list is empty or a singleton
                spots_channels = sample_channels[0..-2]
                Nextflow.log.debug "Spot channels from sample channels ${sample_channels} - last channel -> ${spots_channels}"
            }
        }
        def spots_subpath = params.spots_subpath ?: ''
        def spots_scale = params.spots_scale ?: ''
        if (!spots_subpath && !spots_scale && spots_channels.empty) {
            return [
                ['', '', ''],  // empty subpath, empty resultnane - the input image container contains the array dataset
            ]
        }
        if (spots_subpath) {
            def spots_result_name = "spots-rsfish-${spots_subpath.replace('/', '-')}.csv"
            return [
                [
                    "${meta.stitched_dataset}/${spots_subpath}",
                    spots_result_name,
                    params.spots_image_subpath_ref ? "${meta.stitched_dataset}/${params.spots_image_subpath_ref}" : '',
                ]
            ]
        }
        if (spots_channels.empty) {
            def spots_result_name = "spots-rsfish-${spots_scale}.csv"
            return [
                [
                    "${meta.stitched_dataset}/${spots_scale}",
                    spots_result_name,
                    params.spots_image_subpath_ref ? "${meta.stitched_dataset}/${params.spots_image_subpath_ref}" : '',
                ]
            ]
        } else {
            return spots_channels
                .collect { ch ->
                    // when channel and scale is used we also prepend the stitched dataset
                    def dataset = "${ch}/${spots_scale}"
                    def r = [
                        "${meta.stitched_dataset}/${dataset}",
                        "spots-rsfish-${ch}.csv",
                        params.spots_image_subpath_ref ? "${meta.stitched_dataset}/${params.spots_image_subpath_ref}" : '',
                    ]
                    Nextflow.log.debug "Spot dataset: $r"
                    r
            }
        }

    }
}
