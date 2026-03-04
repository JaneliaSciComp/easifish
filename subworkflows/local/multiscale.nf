include { OMEZARRTOOLS_MULTISCALE } from '../../modules/janelia/omezarrtools/multiscale'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MULTISCALE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow MULTISCALE {
    take:
    ch_meta       // ch: [ meta, img_container, img_dataset ]
    ch_dask_info  // ch: [ dask_scheduler, dask_config ]
    skip_flag     // boolean
    cpus          // number
    mem_gb        // number

    main:
    def multiscale_results
    if (skip_flag) {
        multiscale_results = ch_meta
        multiscale_results.view { it -> log.debug "Skip multiscale pyramid: $it" }
    } else {
        multiscale_results = OMEZARRTOOLS_MULTISCALE(
            ch_meta,
            ch_dask_info,
            cpus,
            mem_gb,
        ).data
        multiscale_results.view { it -> log.debug "Multiscale pyramid: $it" }
    }

    emit:
    done = multiscale_results
}
