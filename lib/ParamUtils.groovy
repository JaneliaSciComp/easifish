import nextflow.Nextflow

class ParamUtils {

    static def get_params_as_list_of_files(List<String> lparams) {
        lparams
            .findAll { cond -> cond }
            .collect { f -> file(f) }
    }

    static def get_warped_subpaths(params) {
        def warped_channels_param = params.warped_channels ?: params.channels
        def warped_scales_param = params.warped_scales ?: params.local_scale

        if (params.warped_subpaths) {
            return ParamUtils.as_list(params.warped_subpaths)
                .collect { warped_subpath_param ->
                    def (fix_subpath, warped_subpath) = warped_subpath_param.tokenize(':')
                    def r = [
                        fix_subpath,
                        warped_subpath ?: fix_subpath,
                    ]
                    Nextflow.log.debug "Warped subpath: $r"
                    r
                }
        } else if (warped_channels_param && warped_scales_param) {
            def warped_scales = ParamUtils.as_list(warped_scales_param)
            def warped_channels = ParamUtils.as_list(warped_channels_param)
            return [warped_channels, warped_scales]
                .combinations()
                .collect { warped_ch, warped_scale ->
                    def r = [
                        "${warped_ch}/${warped_scale}", // fixed subpath
                        "${warped_ch}/${warped_scale}", // warped subpath
                    ]
                    Nextflow.log.debug "Warped subpath: $r"
                    r

            }
        } else {
            return []
        }
    }

    static def get_warped_and_output_channels(params, warped_channels_mapping) {
        def warped_channels_param = params.warped_channels ?: params.channels

        return ParamUtils.as_list(warped_channels_param)
            .collect { ch ->
                def sch = ch as String // just to make sure ch is a String
                if (warped_channels_mapping && (sch in warped_channels_mapping) ) {
                    [ sch, warped_channels_mapping[sch] ]
                } else {
                    // if no mapping defined for the current channel - use the same channel in the output
                    [ sch, sch ]
                }
            }
    }

    static def as_list(v) {
        def vlist
        if (v instanceof Collection) {
            return v
        } else if (v) {
            return v.tokenize(',').collect { it.trim() }
        } else {
            return []
        }
    }
}
