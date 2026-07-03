import nextflow.Nextflow

class ParamUtils {

    static def get_params_as_list_of_files(List<String> lparams) {
        lparams
            .findAll { cond -> cond }
            .collect { f -> Nextflow.file(f) }
    }

    static def get_warped_subpaths(params) {
        def warped_subpaths = params.warped_subpaths ?: params.mov_local_subpath

        if (warped_subpaths) {
            return ParamUtils.as_list(warped_subpaths)
                .collect { warped_subpath_param ->
                    def (fix_subpath, warped_subpath) = warped_subpath_param.tokenize(':')
                    def r = [
                        fix_subpath,
                        warped_subpath ?: fix_subpath,
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

    static def as_bool(v) {
        return Boolean.valueOf(v)
    }

    static def as_double(v) {
        if (v == null || "$v".trim() == '') {
            return 0
        } else {
            return BigDecimal("$v").doubleValue()
        }
    }

    static def as_int(v) {
        if (v == null || "$v".trim() == '') {
            return 0
        } else {
            return BigDecimal("$v").intValue()
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

    static def get_mem_gb(mem_gb, ncpus, default_mem_gb_per_cpu, safety_margin) {
        return mem_gb ?: default_mem_gb_per_cpu * ncpus - safety_margin
    }
}
