class ParamUtils {

    // Function to ensure that resource requirements don't go beyond a maximum limit
    static def check_max(obj, type, params) {
        if (type == 'memory') {
            try {
                if (obj.compareTo(params.max_memory as nextflow.util.MemoryUnit) == 1)
                    return params.max_memory as nextflow.util.MemoryUnit
                else
                    return obj
            } catch (all) {
                println "   ### ERROR ###   Max memory '${params.max_memory}' is not valid! Using default value: $obj"
                return obj
            }
        } else if (type == 'time') {
            try {
                if (obj.compareTo(params.max_time as nextflow.util.Duration) == 1)
                    return params.max_time as nextflow.util.Duration
                else
                    return obj
            } catch (all) {
                println "   ### ERROR ###   Max time '${params.max_time}' is not valid! Using default value: $obj"
                return obj
            }
        } else if (type == 'cpus') {
            try {
                return Math.min(obj, params.max_cpus as int)
            } catch (all) {
                println "   ### ERROR ###   Max cpus '${params.max_cpus}' is not valid! Using default value: $obj"
                return obj
            }
        }
    }

    static def getParamsAsListOfFiles(List<String> lparams) {
        lparams
            .findAll { cond -> cond }
            .collect { f -> file(f) }
    }

    static def get_warped_subpaths(params) {
        def warped_channels_param = params.warped_channels ?: params.channels
        def warped_scales_param = params.warped_scales ?: params.local_scale

        if (params.warped_subpaths) {
            as_list(params.warped_subpaths)
                .collect { warped_subpath_param ->
                    def (fix_subpath, warped_subpath) = warped_subpath_param.tokenize(':')
                    def r = [
                        fix_subpath,
                        warped_subpath ?: fix_subpath,
                    ]
                    log.debug "Warped subpath: $r"
                    r
                }
        } else if (warped_channels_param && warped_scales_param) {
            def warped_scales = as_list(warped_scales_param)
            def warped_channels = as_list(warped_channels_param)
            [warped_channels, warped_scales]
                .combinations()
                .collect { warped_ch, warped_scale ->
                    def r = [
                        "${warped_ch}/${warped_scale}", // fixed subpath
                        "${warped_ch}/${warped_scale}", // warped subpath
                    ]
                    log.debug "Warped subpath: $r"
                    r

            }
        } else {
            []
        }
    }

    static def get_warped_and_output_channels(params, warped_channels_mapping) {
        def warped_channels_param = params.warped_channels ?: params.channels

        as_list(warped_channels_param)
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

}
