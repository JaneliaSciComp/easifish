include { SPARK_START                                   } from '../janelia/spark_start/main'
include { SPARK_STOP                                    } from '../janelia/spark_stop/main'

include { BIGSTITCHER_MODULE as CREATE_DATASET           } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as RESAVE                   } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as DETECT_IP                } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as STITCH_PHASE             } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as STITCH_IP                } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as DUPLICATE_TF             } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as INTENSITY_MATCH          } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as INTENSITY_SOLVE          } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as MATCH_IP                 } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as SOLVER                   } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as CREATE_CONTAINER         } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as FUSE                     } from '../../modules/janelia/bigstitcher/module'

workflow BIGSTITCHER {

    take:
    ch_acquisition_data            // channel: [ meta, files ]
    with_spark_cluster             // boolean: use a distributed spark cluster
    stitching_result_dir           // stitching output dir
    stitched_image_name            // stitched container name
    skip                           // boolean:
    bigstitcher_config             // BigStitcher advanced config as a YAML file
    stitching_steps                // list[string] - BigStitcher steps
    spark_workdir                  // string|file: spark work dir
    spark_local_dir                // string|file: spark local dir
    spark_workers                  // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_spark_workers              // int: min required spark workers
    spark_worker_cpus              // int: number of cpus per worker
    spark_worker_mem_gb            // int: number of GB of memory per worker
    spark_executor_cpus            // int: number of cpus per executor
    spark_executor_mem_gb          // int: number of GB of memory per executor
    spark_executor_overhead_mem_gb // int: executor memory overhead in GB
    spark_task_cpus                // int: cpus allocated per task
    spark_driver_cpus              // int: number of cpus for the driver
    spark_driver_mem_gb            // int: number of GB of memory for the driver
    spark_gb_per_core              // int: number of GB of memory per worker core
    spark_config                   // map: additional spark configuration

    main:
    def prepared_data = ch_acquisition_data
    .map { it ->
        def (meta, files) = it
        def stitching_meta = meta.clone()

        stitching_meta.session_work_dir = "${spark_workdir}/${meta.id}"
        stitching_meta.stitching_result_dir = stitching_result_dir
        stitching_meta.stitched_dataset = meta.id
        stitching_meta.stitching_container = stitched_image_name ?: "fused.ome.zarr"

        def to_lowercase_image_name = stitching_meta.stitching_container.toLowerCase()
        if (to_lowercase_image_name.endsWith('.n5')) {
            stitching_meta.stitching_container_storage = 'N5'
        } else if (to_lowercase_image_name.endsWith('.h5') ||
                   to_lowercase_image_name.endsWith('.hdf5')) {
            stitching_meta.stitching_container_storage = 'HDF5'
        } else if (to_lowercase_image_name.endsWith('.zarr3')) {
            stitching_meta.stitching_container_storage = 'ZARR'
        } else {
            // default to OME-ZARR v2
            stitching_meta.stitching_container_storage = 'ZARR2'
        }

        def data_files = files + [ stitching_result_dir ] +
                         (spark_local_dir ? [file(spark_local_dir)] : [])

        stitching_meta.stitching_xml = files.find { fn -> fn.extension == 'xml' }
        if (stitching_meta.stitching_xml == null) {
            log.debug 'No XML project found for BigStitcher.'
        } else {
            log.debug "Stitching project file was set: ${stitching_meta.stitching_xml}"
            data_files << file(stitching_meta.stitching_xml).parent
        }
        def r = [ stitching_meta, data_files ]
        log.debug "Prepared stitching input: $r"
        r
    }

    def stitching_results
    if (skip || stitching_steps.empty) {
        stitching_results = prepared_data.map { meta, _data_files ->
            meta
        }
    } else {
        def stitching_input = SPARK_START(
            prepared_data,
            spark_config,
            with_spark_cluster,
            spark_workdir,
            spark_local_dir,
            spark_workers,
            min_spark_workers,
            spark_worker_cpus,
            spark_worker_mem_gb,
            spark_executor_cpus,
            spark_executor_mem_gb,
            spark_executor_overhead_mem_gb,
            spark_task_cpus,
            spark_driver_cpus,
            spark_driver_mem_gb,
            spark_gb_per_core,
        )
        .join(prepared_data, by: 0)

        stitching_input.view { it -> log.debug "Stitching input: $it" }

        def stitching_data = stitching_input

        if (has_step('createDataset', stitching_steps)) {
            def inp = bigstitcher_step_input('createDataset', stitching_data, bigstitcher_config)
            stitching_data = CREATE_DATASET(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('resave', stitching_steps)) {
            def inp = bigstitcher_step_input('resave', stitching_data, bigstitcher_config)
            stitching_data = RESAVE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('detectInterestPoints', stitching_steps)) {
            def inp = bigstitcher_step_input('detectInterestPoints', stitching_data, bigstitcher_config)
            stitching_data = DETECT_IP(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('phaseCorrelationStitcher', stitching_steps)) {
            def inp = bigstitcher_step_input('phaseCorrelationStitcher', stitching_data, bigstitcher_config)
            stitching_data = STITCH_PHASE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('interestPointsStitcher', stitching_steps)) {
            def inp = bigstitcher_step_input('interestPointsStitcher', stitching_data, bigstitcher_config)
            stitching_data = STITCH_IP(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('matchInterestPoints', stitching_steps)) {
            def inp = bigstitcher_step_input('matchInterestPoints', stitching_data, bigstitcher_config)
            stitching_data = MATCH_IP(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('solver', stitching_steps)) {
            def inp = bigstitcher_step_input('solver', stitching_data, bigstitcher_config)
            stitching_data = SOLVER(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('duplicateTransformation', stitching_steps)) {
            def inp = bigstitcher_step_input('duplicateTransformation', stitching_data, bigstitcher_config)
            stitching_data = DUPLICATE_TF(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('intensityMatch', stitching_steps)) {
            def inp = bigstitcher_step_input('intensityMatch', stitching_data, bigstitcher_config)
            stitching_data = INTENSITY_MATCH(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('intensitySolver', stitching_steps)) {
            def inp = bigstitcher_step_input('intensitySolver', stitching_data, bigstitcher_config)
            stitching_data = INTENSITY_SOLVE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('createContainer', stitching_steps)) {
            def inp = bigstitcher_step_input('createContainer', stitching_data, bigstitcher_config)
            stitching_data = CREATE_CONTAINER(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        if (has_step('fuse', stitching_steps)) {
            def inp = bigstitcher_step_input('fuse', stitching_data, bigstitcher_config)
            stitching_data = FUSE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }

        stitching_results = SPARK_STOP(
            stitching_data.map { meta, spark, _files -> [meta, spark] },
            with_spark_cluster,
        )
        .map { it ->
            def (meta, spark) = it
            log.debug "Stopped spark ${spark} - stitching result: $meta"
            meta
        }
    }

    emit:
    done = stitching_results
}

// --- Helper methods ---

def get_stitching_xml_or_default(meta) {
    return meta.stitching_xml ?: "${meta.image_dir}/dataset.xml"
}

def normalize_step_name(String step_name) {
    def canonical = [stitch: 'phaseCorrelationStitcher']
    return canonical[step_name] ?: step_name
}

def has_step(String step_name, List steps) {
    if (step_name in steps) return true
    def aliases = [
        stitch: 'phaseCorrelationStitcher',
        phaseCorrelationStitcher: 'stitch',
    ]
    return aliases[step_name] in steps
}

def get_step_config(Map config, String step_name) {
    def normalized = normalize_step_name(step_name)
    config?.get(normalized) ?: [:]
}

def get_step_class(Map config, String step_name) {
    def cls = get_step_config(config, step_name).get('classname')
    if (!cls) {
        error "Missing BigStitcher classname for step '${step_name}' in bigstitcher_config"
    }
    return cls
}

def get_advanced_args(Map config, String step_name) {
    get_step_config(config, step_name).get('advancedArgs') ?: []
}

/**
 * Prepares [class, params] for a given BigStitcher step.
 * Handles step-specific arguments (e.g. createDataset needs input paths,
 * createContainer needs output container info).
 */
def prepare_bigstitcher_args(String step_name, Map config, meta) {
    def cls = get_step_class(config, step_name)
    def stitching_xml = get_stitching_xml_or_default(meta)
    def intensity_location = "stitching/${meta.id}/intensity/"
    def intensity_coefficients = "${intensity_location}coefficients.zarr"
    def params
    if (step_name == 'createDataset') {
        params = [
            '--input-pattern', meta.pattern,
            '--input-path', meta.image_dir,
            '-x', stitching_xml,
        ]
    } else if (step_name == 'resave') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.image_dir}/dataset.zarr",
            '-s', meta.stitching_container_storage,
        ]
    } else if (step_name == 'createContainer') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
            '--group', meta.id,
            '-s', meta.stitching_container_storage,
        ]
    } else if (step_name == 'intensityMatch') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.stitching_result_dir}/${intensity_location}",
        ]
    } else if (step_name == 'intensitySolver') {
        params = [
            '-x', stitching_xml,
            '--matchesPath', "${meta.stitching_result_dir}/${intensity_location}",
            '-o', "${meta.stitching_result_dir}/${intensity_coefficients}",
            '-s', meta.stitching_container_storage,
        ]
        // update config for fuse step and set 'useIntensityCoefficients' automatically
        // if the intensity correction is not needed it can still be ignored by setting 'ignoreIntensityCoefficients' to true
        def fuse_config = get_step_config(config, 'fuse')
        if (fuse_config) {
            fuse_config['useIntensityCoefficients'] = true
        } else {
            fuse_config['useIntensityCoefficients'] = true
            config['fuse'] = fuse_config
        }
    } else if (step_name == 'fuse') {
        def intensityCoefficientsArgs = get_step_config(config, step_name)['useIntensityCoefficients'] && !get_step_config(config, step_name)['ignoreIntensityCoefficients']
            ? [
                '--intensityN5Path', "${meta.stitching_result_dir}/${intensity_coefficients}",
                '--intensityN5Storage', meta.stitching_container_storage,
              ]
            : []
        params = [
            '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
            '--group', meta.id,
            '-s', meta.stitching_container_storage,
        ] + intensityCoefficientsArgs
    } else {
        params = ['-x', stitching_xml]
    }

    params += get_advanced_args(config, step_name)
    log.debug "BigStitcher step '${step_name}': class=${cls}, params=${params}"
    [cls, params]
}

/**
 * Prepares multiMap channels for a BigStitcher step invocation.
 * Returns an object with .module_args, .data_files, and .carry channels.
 * After the step runs, join its output with .carry to restore [meta, spark, files].
 */
def bigstitcher_step_input(String step_name, ch_data, Map config) {
    ch_data.multiMap { meta, spark, files ->
        def (cls, step_params) = prepare_bigstitcher_args(step_name, config, meta)
        module_args: [meta, spark, cls, step_params]
        data_files: files
        carry: [meta, files]
    }
}
