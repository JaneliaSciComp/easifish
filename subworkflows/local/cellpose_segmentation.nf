include { CELLPOSE   } from '../../modules/janelia/cellpose/main.nf'

include { DASK_START } from '../janelia/dask_start/main.nf'
include { DASK_STOP  } from '../janelia/dask_stop/main.nf'

workflow CELLPOSE_SEGMENTATION {
    take:
    ch_meta                // channel: [ meta, 
                           //            img_container_dir, img_dataset,
                           //            output_dir, 
                           //            segmentation_container, segmentation_dataset ]
    skip                   // boolean: if true skip segmentation completely and just return the meta as if it ran
    models_dir             // string|file: directory
    log_config             // string|file: log configuration file
    distributed            // boolean: use a distributed dask cluster
    dask_config            // string|file: dask configuration file
    work_dir               // string|file: dask work dir
    dask_workers           // int: number of workers in the cluster (ignored if distributed is false)
    dask_min_workers       // int: min required dask workers
    dask_worker_cpus       // int: number of cores per worker
    dask_worker_mem_gb     // int: number of GB of memory per worker
    segmentation_cpus      // int: number of cores to use for segmentation main process
    segmentation_mem_gb    // int: number of GB of memory to use for segmentation main process

    main:
    def final_segmentation_results
    if (!skip) {
        def segmentation_prep_inputs = ch_meta
        | multiMap {
            def (meta, img_container_dir, img_dataset, output_dir, segmentation_container, segmentation_dataset) = it
            log.debug "Start to prepare inputs for cellpose segmentation: $it"
            def segmentation_work_dir = work_dir 
                ? "${work_dir}/${meta.id}"
                : "${output_dir}/${meta.id}"

            def segmentation_meta = meta + [segmentation_work_dir: segmentation_work_dir]
            def cellpose_data = [
                segmentation_meta,
                img_container_dir,
                img_dataset,
                models_dir,
                "${output_dir}/${segmentation_container}",
                segmentation_dataset,
                segmentation_work_dir,
            ]
            def cluster_data = [
                segmentation_meta,
                [ 
                    img_container_dir,
                    output_dir,
                ],
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
        dask_cluster.subscribe {
            log.debug "Dask cluster info: $it"
        }

        def segmentation_inputs = dask_cluster
        | join(segmentation_prep_inputs.cellpose_data, by: 0)
        | multiMap {
            def (meta, cluster_context, img_container_dir, img_dataset, models_dir_param, segmentation_container_dir, segmentation_dataset, segmentation_work_dir) = it
            def cellpose_data = [
                meta,
                img_container_dir,
                img_dataset,
                models_dir_param ? file(models_dir_param) : [],
                segmentation_container_dir,
                segmentation_dataset,
                segmentation_work_dir,
            ]
            def cluster_info = [
                cluster_context.scheduler_address,
                dask_config ? file(dask_config) : [],

            ]
            log.debug "cellpose segmentation input: $it -> {cellpose: ${cellpose_data}, cluster: ${cluster_info}}"
            cellpose_data: cellpose_data
            cluster_info: cluster_info
        }

        def segmentation_results = CELLPOSE(
            segmentation_inputs.cellpose_data,
            segmentation_inputs.cluster_info,
            log_config ? file(log_config) : [],
            segmentation_cpus,
            segmentation_mem_gb,
        )
        segmentation_results.results.subscribe {
            log.debug "Segmentation results: $it"
        }

        dask_cluster.join(segmentation_results.results, by:0)
        | map {
            def (meta, cluster_context) = it
            [ meta, cluster_context ]
        }
        | groupTuple
        | DASK_STOP

        final_segmentation_results = segmentation_results.results
    } else {
        // FIXME:
        final_segmentation_results = ch_meta
    }

    emit:
    done = final_segmentation_results
}
