include { MULTISCALE_PYRAMID } from '../../modules/local/multiscale/pyramid/main'

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

    main:
    def multiscale_results
    if (skip_flag) {
        multiscale_results = ch_meta
        multiscale_results.subscribe { log.debug "Skip multiscale pyramid: $it" }
    } else {
        MULTISCALE_PYRAMID(
            ch_meta,
            ch_dask_info,
            params.multiscale_cpus,
            params.multiscale_mem_gb,
        )
        multiscale_results = MULTISCALE_PYRAMID.out.data
        multiscale_results.subscribe { log.debug "Multiscale pyramid: $it" }
    }

    emit:
    done = multiscale_results
}
