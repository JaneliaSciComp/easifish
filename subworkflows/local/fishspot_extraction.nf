include { FISHSPOTS  } from '../../modules/local/fishspots'

include { DASK_START } from '../janelia/dask_start'
include { DASK_STOP  } from '../janelia/dask_stop'

workflow FISHSPOT_EXTRACTION {
    take:
    ch_spots_input             // ch: [ meta, input_img, input_subpath, spots_output_dir, spots_output_name ]
    distributed                // boolean
    fishspots_config           // String|file fishspots config
    dask_config                // String|file dask config
    workdir
    dask_workers
    dask_min_workers           // int: min required spark workers
    dask_worker_cpus           // int: number of cores per worker
    dask_worker_mem_gb         // int: number of GB of memory per worker core
    fishspots_cpus             // int: number of cores for the driver
    fishspots_mem_gb           // int: number of GB of memory for the driver

    main:
    def dask_data = ch_spots_input
    | map {
        def (meta, input_img, input_subpath, spots_output_dir, spots_output_name) = it
        [
            meta,
            [ input_img, spots_output_dir],
        ]
    }

    def dask_cluster = DASK_START(
        dask_data,
        distributed,
        dask_config,
        workdir,
        dask_workers,
        dask_min_workers,
        dask_worker_cpus,
        dask_worker_mem_gb,
    )

    dask_cluster.subscribe {
        log.debug "Dask cluster info: $it"
    }

    def fishspots_inputs = dask_cluster
    | join(ch_spots_input, by: 0)
    | multiMap {
        def (meta, cluster_context,
             input_img, input_subpath, spots_output_dir, spots_output_name) = it

        def fishspots_data = [
            meta, input_img, input_subpath, spots_output_dir, spots_output_name,
        ]
        def cluster_info = [
            cluster_context.scheduler_address,
            dask_config ? file(dask_config) : [],
        ]
        log.debug "Prepare fishspots input $it"
        fishspots_data: fishspots_data
        cluster_info: cluster_info
    }

    def fishspots_outputs = FISHSPOTS(
        fishspots_inputs.fishspots_data,
        fishspots_inputs.cluster_info,
        fishspots_config ? file(fishspots_config) : [],
        fishspots_cpus,
        fishspots_mem_gb,
    )

    def fishspots_results = FISHSPOTS.out.params
    | join(FISHSPOTS.out.csv, by: 0)
    | map {
        def (meta, input_image, input_dataset, spots_output_dir, spots_result_name, dask_scheduler, full_output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            full_output_filename,
            dask_scheduler,
        ]
    }

    def final_fishspots_results = fishspots_results
    | map {
        log.debug "fishspots results: $it"

        def (meta, input_image, input_dataset, full_output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            full_output_filename,
        ]
    }

    def all_fishspots_results = fishspots_results
    | groupTuple(by: [0, 4]) // group by meta and spark

    dask_cluster.join(all_fishspots_results, by:0)
    | map {
        def (meta, cluster_context) = it
        [ meta, cluster_context ]
    }
    | DASK_STOP

    emit:
    done = final_fishspots_results

}
