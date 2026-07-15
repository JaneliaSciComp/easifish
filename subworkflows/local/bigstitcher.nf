include { SPARK_START                          } from '../janelia/spark_start'
include { SPARK_STOP                           } from '../janelia/spark_stop'

include { SPARK_RUNAPP as CREATE_DATASET       } from '../janelia/spark_start'
include { SPARK_RUNAPP as RESAVE               } from '../janelia/spark_start'
include { SPARK_RUNAPP as ESTIMATE_FLATFIELD   } from '../janelia/spark_start'
include { SPARK_RUNAPP as FLATFIELD_CORRECTION } from '../janelia/spark_start'
include { SPARK_RUNAPP as SOLVE_PAIRS          } from '../janelia/spark_start'
include { SPARK_RUNAPP as DETECT_IPS           } from '../janelia/spark_start'
include { SPARK_RUNAPP as MATCH_IPS            } from '../janelia/spark_start'
include { SPARK_RUNAPP as SOLVE_IPS            } from '../janelia/spark_start'
include { SPARK_RUNAPP as REFINE_IPS           } from '../janelia/spark_start'
include { SPARK_RUNAPP as SOLVE_REFINED_IPS    } from '../janelia/spark_start'
include { SPARK_RUNAPP as DUPLICATE_TF         } from '../janelia/spark_start'
include { SPARK_RUNAPP as INTENSITY_MATCH      } from '../janelia/spark_start'
include { SPARK_RUNAPP as INTENSITY_SOLVE      } from '../janelia/spark_start'
include { SPARK_RUNAPP as CREATE_CONTAINER     } from '../janelia/spark_start'
include { SPARK_RUNAPP as FUSE                 } from '../janelia/spark_start'

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
        def current_step

        current_step = matched_step('createDataset', stitching_steps)
        if (current_step) {
            log.debug "Run 'createDataset'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = CREATE_DATASET(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'createDataset' - not configured"
        }
        current_step = matched_step('resave', stitching_steps)
        if (current_step) {
            log.debug "Run 'resave'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = RESAVE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'resave' - not configured"
        }
        current_step = matched_step('estimateFlatField', stitching_steps)
        if (current_step) {
            log.debug "Run 'estimateFlatField'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = ESTIMATE_FLATFIELD(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'estimateFlatField' - not configured"
        }
        current_step = matched_step('flatfieldCorrection', stitching_steps)
        if (current_step) {
            log.debug "Run 'flatfieldCorrection'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = FLATFIELD_CORRECTION(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'flatfieldCorrection' - not configured"
        }
        current_step = matched_step('detectInterestPoints', stitching_steps)
        if (current_step) {
            log.debug "Run 'detectInterestPoints'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = DETECT_IPS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'detectInterestPoints' - not configured"
        }
        current_step = matched_step('solvePairs', stitching_steps)
        if (current_step) {
            log.debug "Run 'solvePairs'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = SOLVE_PAIRS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'solvePairs' - not configured"
        }
        current_step = matched_step('matchInterestPoints', stitching_steps)
        if (current_step) {
            log.debug "Run 'matchInterestPoints'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = MATCH_IPS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'matchInterestPoints' - not configured"
        }
        current_step = matched_step('solveIPs', stitching_steps)
        if (current_step) {
            log.debug "Run 'solveIPs'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = SOLVE_IPS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        current_step = matched_step('refineInterestPointsMatches', stitching_steps)
        if (current_step) {
            log.debug "Run 'refineInterestPointsMatches'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = REFINE_IPS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        current_step = matched_step('solveRefinedIPS', stitching_steps)
        if (current_step) {
            log.debug "Run 'solveRefinedIPS'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = SOLVE_REFINED_IPS(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        }
        current_step = matched_step('duplicateTransformation', stitching_steps)
        if (current_step) {
            log.debug "Run 'duplicateTransformation'"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = DUPLICATE_TF(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'duplicateTransformation' - not configured"
        }
        current_step = matched_step('intensityMatch', stitching_steps)
        if (current_step) {
            log.debug "Run 'intensityMatch' - not configured"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = INTENSITY_MATCH(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'intensityMatch' - not configured"
        }
        current_step = matched_step('intensitySolve', stitching_steps)
        if (current_step) {
            log.debug "Run 'intensitySolve' - not configured"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = INTENSITY_SOLVE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'intensitySolve' - not configured"
        }
        current_step = matched_step('createContainer', stitching_steps)
        if (current_step) {
            log.debug "Run 'createContainer' - not configured"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = CREATE_CONTAINER(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'createContainer' - not configured"
        }
        current_step = matched_step('fuse', stitching_steps)
        if (current_step) {
            log.debug "Run 'fuse' - not configured"
            def inp = bigstitcher_step_input(current_step, stitching_data, bigstitcher_config)
            stitching_data = FUSE(inp.module_args, inp.data_files).join(inp.carry, by: 0)
        } else {
            log.debug "Skip 'fuse' - not configured"
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
    def canonical = [
        stitch: 'solvePairs',
    ]
    return canonical[step_name] ?: step_name
}

def matched_step(String step_name, List steps) {
    if (step_name in steps)
        return step_name
    def aliases = [
        solvePairs: ['stitch'],
    ]
    return (aliases[step_name] ?: []).find { s -> s in steps }
}

def get_step_config(Map config, String step_name) {
    def step_config = config?.get(step_name)
    if (step_config) {
        log.debug "Use ${step_name} config: ${step_config}"
        return step_config
    } else {
        def normalized_step = normalize_step_name(step_name)
        log.debug "Use normalized step ${normalized_step} config for ${step_name}: ${step_config}"
        config?.get(normalized_step) ?: [:]
    }
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
            '--input-pattern', "${meta.pattern}",
            '--input-path', "${meta.image_dir}",
            '-x', stitching_xml,
        ]
    } else if (step_name == 'resave') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.image_dir}/dataset.zarr",
        ]
    } else if (step_name == 'estimateFlatField') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.image_dir}/corrections.zarr",
        ]
    } else if (step_name == 'flatfieldCorrection') {
        params = [
            '--fields', "${meta.image_dir}/corrections.zarr",
            '-x', stitching_xml,
            '-xo', stitching_xml,
            '-o', "${meta.image_dir}/dataset-ff.zarr",
        ]
    } else if (step_name == 'createContainer') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
            '--group', meta.id,
        ]
    } else if (step_name == 'intensityMatch') {
        params = [
            '-x', stitching_xml,
            '-o', "${meta.stitching_result_dir}/${intensity_location}",
        ]
    } else if (step_name == 'intensitySolve') {
        params = [
            '-x', stitching_xml,
            '--matchesPath', "${meta.stitching_result_dir}/${intensity_location}",
            '-o', "${meta.stitching_result_dir}/${intensity_coefficients}",
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
              ]
            : []
        params = [
            '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
            '--group', meta.id,
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
        def (step_cls, step_args) = prepare_bigstitcher_args(step_name, config, meta)

        module_args: [
            meta, spark, spark.work_dir,
            [] /*no app jar path*/, '' /*use default app jar*/, [:] /* no additional spark config */,
            step_cls, step_args
        ]
        data_files: files
        carry: [meta, files]
    }
}
