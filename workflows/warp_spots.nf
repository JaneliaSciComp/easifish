/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT WARPING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow WARP_SPOTS {
    take:
    registration_results        // channel:
    spot_extraction_results     // channel:
    outdir                      // file|string - output dir

    main:
    registration_results | view

    spot_extraction_results | view

    emit:

}
