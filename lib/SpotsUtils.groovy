import nextflow.Nextflow

class SpotsUtils {

    /**
     * Get spots channels either from the meta or from the params
     * If excluded channels are specified it excludes these
     * otherwise it excludes the DAPI channel
     * The DAPI channel can be either specified explicitly or
     * implicitly the last channel
     */
    /**
     * True if the spots channels were explicitly set to empty,
     * which means there are no spots channels
     * (as opposed to not being set at all)
     */
    static boolean has_empty_spots_channels(meta) {
        return meta.spots_channels != null && !(meta.spots_channels as String).trim()
    }

    static def get_spots_channels(meta, params) {
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
        if (has_empty_spots_channels(meta)) {
            spots_channels = []
            Nextflow.log.debug "Spot channels for ${meta.id} explicitly set to empty - no spots channels"
        } else if (meta.spots_channels) {
            spots_channels = ParamUtils.as_list(meta.spots_channels)
            Nextflow.log.debug "Spot channels for ${meta.id}: ${spots_channels}"
        } else if (params.spots_channels) {
            spots_channels = ParamUtils.as_list(params.spots_channels)
            Nextflow.log.debug "Spot channels param: ${spots_channels}"
        } else {
            def excluded_channels = params.spots_excluded_channels
                ? ParamUtils.as_list(params.spots_excluded_channels as String)
                : []
            if (!excluded_channels.empty) {
                spots_channels = sample_channels.findAll { sc ->  !(sc in excluded_channels) }
                Nextflow.log.debug "Spot channels from sample channels ${sample_channels} without excluded channel ${excluded_channels} -> ${spots_channels}"
            } else if (dapi_channel) {
                // dapi_channel may be an integer if it comes from params
                spots_channels = sample_channels.findAll { sc -> sc != (dapi_channel as String) }
                Nextflow.log.debug "Spot channels from sample channels ${sample_channels} without DAPI channel ${dapi_channel} -> ${spots_channels}"
            } else {
                // automatically consider DAPI the last channel
                // so return all but the last channel
                // this may throw an exception if the channel list is empty or a singleton
                spots_channels = sample_channels[0..-2]
                Nextflow.log.debug "Spot channels from sample channels ${sample_channels} - last channel -> ${spots_channels}"
            }
        }
        return spots_channels
    }

}
