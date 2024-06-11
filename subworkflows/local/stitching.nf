
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
    acquisition_data // [ meta, files ]
    workdir
    with_spark_cluster
    with_flatfield_correction
    spark_workers
    spark_worker_cores
    spark_gb_per_core
    spark_driver_cores
    spark_driver_mem_gb

    main:
    STITCHING_PREPARE(
        acquisition_data
    )

    def stitching_input = SPARK_START(
        STITCHING_PREPARE.out, // [meta, data_paths]
        workdir,
        with_spark_cluster,
        spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_mem_gb
    )
    | join(acquisition_data, by:0)
    | map {
        def (meta, spark, files) = it
        def r = [
            meta, files, spark,
        ]
        log.debug "Stitching input: $it -> $r"
        r
    }

    STITCHING_PARSECZI(stitching_input)

    STITCHING_CZI2N5(STITCHING_PARSECZI.out.acquisitions)

    def ready_for_flatfield = STITCHING_CZI2N5.out
    | combine(as_value_channel(with_flatfield_correction))
    | branch {
        def (meta, files, spark, flatfield_correction) = it
        with_flatfield: flatfield_correction
                        [ meta, files, spark ]
        without_flatfield: !flatfield_correction
                        [ meta, files, spark ]
    }

    def flatfield_results = STITCHING_FLATFIELD(ready_for_flatfield.with_flatfield)
        .acquisitions
        .mix(ready_for_flatfield.without_flatfield)

    STITCHING_STITCH(flatfield_results)

    STITCHING_FUSE(STITCHING_STITCH.out.acquisitions)

    def fuse_result = STITCHING_FUSE.out.acquisitions
    | map {
        def (meta, files, spark) = it
        [ meta, spark ]
    }

    def completed_stitching_result = SPARK_STOP(fuse_result, params.spark_cluster)

    completed_stitching_result.subscribe {
        log.debug "Stitching result: $it"
    }

    emit:
    done = completed_stitching_result
}

def as_value_channel(v) {
    if (!v.toString().contains("Dataflow")) {
        Channel.value(v)
    } else if (!v.toString().contains("value")) {
        // if not a value channel return the first value
        v.first()
    } else {
        // this is a value channel
        v
    }
}
