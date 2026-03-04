include { SEGTOOLS_DISTRIBUTED_CELLPOSE    } from '../../modules/janelia/segtools/distributed/cellpose/main.nf'
include { SEGTOOLS_DISTRIBUTED_MERGELABELS } from '../../modules/janelia/segtools/distributed/mergelabels/main.nf'

include { DASK_START                       } from '../janelia/dask_start/main.nf'
include { DASK_STOP                        } from '../janelia/dask_stop/main.nf'

include { MULTISCALE                       } from './multiscale'

workflow CELLPOSE_SEGMENTATION {
    take:
    ch_meta                // channel: [ meta,
                           //            img_container_dir, img_dataset,
                           //            output_dir,
                           //            segmentation_container ]
    skip_segmentation      // boolean: if true skip segmentation completely and just return the meta as if it ran
    run_standalone_merge_labels  // boolean: if true run merge labels (after distributed cellpose if it ran, or standalone otherwise)
    models_dir             // string|file: directory
    model_name             // string model name
    preprocessing_config   // string|file
    log_config             // string|file: log configuration file
    skip_multiscale        // boolean: if true skip multiscale pyramid for segmented image
    distributed            // boolean: use a distributed dask cluster
    dask_config            // string|file: dask configuration file
    work_dir               // string|file: dask work dir
    dask_workers           // int: number of workers in the cluster (ignored if distributed is false)
    dask_min_workers       // int: min required dask workers
    dask_worker_cpus       // int: number of cores per worker
    dask_worker_mem_gb     // int: number of GB of memory per worker
    segmentation_cpus      // int: number of cores to use for segmentation main process
    segmentation_mem_gb    // int: number of GB of memory to use for segmentation main process
    multiscale_cpus        // int: number of CPUs allocated for labels multiscale driver
    multiscale_mem_gb      // int: memory size in GB allocated for the labels multiscale driver

    main:
    def final_segmentation_results
    if (!skip_segmentation || run_standalone_merge_labels || !skip_multiscale) {
        def segmentation_prep_inputs = ch_meta
        | multiMap { it ->
            def (meta, img_container_dir, img_dataset, output_dir, segmentation_container) = it
            log.debug "Start to prepare inputs for cellpose segmentation: $it"
            def segmentation_work_dir = work_dir
                ? file("${work_dir}/${meta.id}/${img_dataset}")
                : file("${output_dir}/${workflow.sessionId}/${meta.id}/${img_dataset}")

            def cellpose_models_dir = models_dir ? file(models_dir) : "${output_dir}/cellpose-models"

            def cellpose_data = [
                meta,
                img_container_dir,
                img_dataset,
                cellpose_models_dir,
                model_name,
                output_dir,
                segmentation_container,
                img_dataset, // use the same dataset for labels as the input dataset
                segmentation_work_dir,
            ]

            def cluster_dirs = [
                img_container_dir,
                output_dir,
            ] + (cellpose_models_dir ? [ cellpose_models_dir ] : [])

            def cluster_data = [
                meta,
                cluster_dirs,
            ]

            log.debug "Prepare input to cellpose segmentation: $it -> {cellpose: ${cellpose_data}, cluster: ${cluster_data}}"
            cellpose_data: cellpose_data
            cluster_data: cluster_data
        }

        def dask_cluster = DASK_START(
            segmentation_prep_inputs.cluster_data,
            distributed,
            dask_config,
            work_dir,
            dask_workers,
            dask_min_workers,
            dask_worker_cpus,
            dask_worker_mem_gb
        )
        dask_cluster.view { it ->
            log.debug "Dask cluster info: $it"
        }

        def segmentation_inputs = dask_cluster
        | join(segmentation_prep_inputs.cellpose_data, by: 0)
        | multiMap { it ->
            def (meta, cluster_context,
                 img_container_dir, img_dataset,
                 cellpose_models_dir, cellpose_model_name,
                 segmentation_output_dir,
                 segmentation_container, segmentation_dataset,
                 segmentation_work_dir) = it
            def cellpose_data = [
                meta,
                img_container_dir,
                img_dataset,
                cellpose_models_dir ?: [],
                cellpose_model_name,
                segmentation_output_dir,
                segmentation_container,
                segmentation_dataset,
                segmentation_work_dir,
            ]
            def cluster_info = [
                cluster_context.scheduler_address,
                dask_config ? file(dask_config) : [],
            ]
            log.debug "Cellpose segmentation input: $it -> {cellpose: ${cellpose_data}, cluster: ${cluster_info}}"
            cellpose_data: cellpose_data
            cluster_info: cluster_info
        }

        // build labels channel from DISTRIBUTEDCELLPOSE or existing output
        def labels_ch
        if (!skip_segmentation) {
            labels_ch = SEGTOOLS_DISTRIBUTED_CELLPOSE(
                segmentation_inputs.cellpose_data,
                segmentation_inputs.cluster_info,
                preprocessing_config ? file(preprocessing_config) : [],
                log_config ? file(log_config) : [],
                segmentation_cpus,
                segmentation_mem_gb,
            ).results
        } else {
            labels_ch = segmentation_inputs.cellpose_data
            | map { it ->
                def (meta,
                     img_container_dir, img_dataset,
                     _cellpose_models_dir, _cellpose_model_name,
                     segmentation_output_dir,
                     segmentation_container, segmentation_dataset,
                     _segmentation_work_dir) = it
                log.debug "Using existing segmentation output for: $meta"
                [
                    meta,
                    img_container_dir, img_dataset,
                    "${segmentation_output_dir}/${segmentation_container}",
                    segmentation_dataset ?: img_dataset,
                ]
            }
        }

        // optionally run merge labels (after cellpose or standalone)
        if (run_standalone_merge_labels) {
            def mergelabels_inputs = labels_ch
            | combine(dask_cluster, by: 0)
            | flatMap { it ->
                def (meta, _input_image, _image_subpath,
                     labels_containers, labels_subpath,
                     cluster_context) = it
                labels_containers.split('\n')
                .findAll { lc -> lc }
                .collect { labels_container ->
                    log.debug "Prepare merge labels input: ${labels_container}"
                    [
                        [
                            meta,
                            file(labels_container),
                            labels_subpath,
                            [],            // output in-place
                            labels_subpath,
                            [],            // no working dir
                        ],
                        [
                            cluster_context.scheduler_address,
                            dask_config ? file(dask_config) : [],
                        ]
                    ]
                }
            }

            final_segmentation_results = SEGTOOLS_DISTRIBUTED_MERGELABELS(
                mergelabels_inputs.map { it[0] },
                mergelabels_inputs.map { it[1] },
                log_config ? file(log_config) : [],
                segmentation_cpus,
                segmentation_mem_gb,
            ).results
        } else {
            final_segmentation_results = labels_ch
        }
        final_segmentation_results.view { it ->
            log.debug "Cellpose results: $it"
        }
        // generate multiscale pyramid for the segmentation results
        def labels_multiscale_inputs = final_segmentation_results
        | combine(dask_cluster, by: 0)
        | flatMap { it ->
            def (meta, _input_container, _input_subpath, labels_containers, labels_subpath, cluster_context) = it
            labels_containers.split('\n')
            .findAll { lit -> lit }
            .collect { labels_container ->
                def r = [
                    meta, labels_container, labels_subpath,
                ]
                def cluster_info = [
                    cluster_context.scheduler_address,
                    dask_config ? file(dask_config) : [],
                ]
                log.info "Prepare cellpose multiscale inputs: $r on $cluster_info"
                [
                    r,
                    cluster_info
                ]
            }
        }

        def labels_multiscale_outputs = MULTISCALE(
            labels_multiscale_inputs.map { it -> it[0] },
            labels_multiscale_inputs.map { it -> it[1] },
            skip_multiscale,
            multiscale_cpus,
            multiscale_mem_gb,
        )

        // wait until all multiscale finish
        def all_multiscale_results = labels_multiscale_outputs
        | groupTuple(by:0)

        // then stop the dask cluster
        dask_cluster.join(all_multiscale_results, by:0)
        | map {
            def (meta, cluster_context) = it
            [ meta, cluster_context ]
        }
        | DASK_STOP
    } else { // skip segmentation
        final_segmentation_results = ch_meta
        | map {
            def (meta, img_container_dir, img_dataset, output_dir, segmentation_container) = it
            log.debug "Skip cellpose segmentation: $it"
            [
                meta,
                img_container_dir, img_dataset,
                "${output_dir}/${segmentation_container}",
            ]
        }
    }

    emit:
    done = final_segmentation_results
}
