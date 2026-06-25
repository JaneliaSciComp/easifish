include { OMEZARRTOOLS_MULTISCALE } from '../../../modules/janelia/omezarrtools/multiscale'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MULTISCALE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow MULTISCALE {
    take:
    ch_meta           // ch: [ meta, img_container, img_dataset ]
    ch_dask_info      // ch: [ dask_scheduler, dask_config ]
    generate_pyramid  // boolean

    main:
    if (generate_pyramid) {
        multiscale_results = OMEZARRTOOLS_MULTISCALE(
            ch_meta,
            ch_dask_info,
        ).data
        multiscale_results.view { it -> log.debug "Multiscale pyramid: $it" }
    } else {
        multiscale_results = ch_meta
        multiscale_results.view { it -> log.debug "Skip multiscale pyramid: $it" }
    }

    emit:
    multiscale_results
}
