
include { SPARK_START            } from '../janelia/spark_start/main'
include { SPARK_STOP             } from '../janelia/spark_stop/main'

include { STITCHING_PREPARE      } from '../../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI     } from '../../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5       } from '../../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD    } from '../../modules/local/stitching/flatfield/main'
include { STITCHING_STITCH       } from '../../modules/local/stitching/stitch/main'
include { STITCHING_FUSE         } from '../../modules/local/stitching/fuse/main'

workflow STITCHING {
    take:
    acquisition_data       // channel: [ meta, files ]
    flatfield_correction   // boolean: run flatfield correction
    with_spark_cluster     // boolean: use a distributed spark cluster
    workdir                // string|file: spark work dir
    spark_workers          // int: number of workers in the cluster (ignored if spark_cluster is false)
    spark_worker_cores     // int: number of cores per worker
    spark_gb_per_core      // int: number of GB of memory per worker core
    spark_driver_cores     // int: number of cores for the driver
    spark_driver_mem_gb    // int: number of GB of memory for the driver

    main:
    def prepared_data = STITCHING_PREPARE(
        acquisition_data
    )

    // def stitching_input = SPARK_START(
    //     prepared_data, // [meta, data_paths]
    //     with_spark_cluster,
    //     workdir,
    //     spark_workers,
    //     spark_worker_cores,
    //     spark_gb_per_core,
    //     spark_driver_cores,
    //     spark_driver_mem_gb
    // )
    // | map { // rearrange input args
    //     def (meta, spark, files) = it
    //     def r = [
    //         meta, files, spark,
    //     ]
    //     log.debug "Stitching input: $it -> $r"
    //     r
    // }

    // STITCHING_PARSECZI(stitching_input)

    // STITCHING_CZI2N5(STITCHING_PARSECZI.out.acquisitions)

    // def flatfield_results
    // if (flatfield_correction) {
    //     flatfield_results = STITCHING_FLATFIELD(STITCHING_CZI2N5.out.acquisitions).acquisitions
    // } else {
    //     flatfield_results = STITCHING_CZI2N5.out.acquisitions
    // }

    // STITCHING_STITCH(flatfield_results)

    // STITCHING_FUSE(STITCHING_STITCH.out.acquisitions)

    // def fuse_result = STITCHING_FUSE.out.acquisitions
    // | map {
    //     def (meta, files, spark) = it
    //     // revert spark map with files for spark_stop
    //     [ meta, spark, files ]
    // }

    // def completed_stitching_result = SPARK_STOP(fuse_result, with_spark_cluster)

    // completed_stitching_result.subscribe {
    //     def (meta, spark, data_paths) = it
    //     def r = meta
    //     log.debug "Stitching result: $it -> $r"
    //     r
    // }

    def completed_stitching_result = prepared_data.map { it[0] }

    emit:
    done = completed_stitching_result // channel: meta
}
