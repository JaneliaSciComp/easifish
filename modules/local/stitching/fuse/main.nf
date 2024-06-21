process STITCHING_FUSE {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/stitching-spark:1.11.0'
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(files), val(spark)
    path(darkfield_file, stageAs: 'ff/*')
    path(flatfield_file, stageAs: 'df/*')

    output:
    tuple val(meta), path(files), val(spark), emit: acquisitions
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def darkfield_file_arg = darkfield_file ? "--darkfield-file ${darkfield_file}" : ''
    def flatfield_file_arg = flatfield_file ? "--flatfield-file ${flatfield_file}" : ''
    def stitching_result_dir_arg = meta.stitching_result_dir ? "-o ${meta.stitching_result_dir}" : ''
    def stitched_container_arg = meta.stitching_container ? "--outputContainerName ${meta.stitching_container}" : ''
    def stitched_dataset_arg = meta.stitched_dataset ? "--outputDatasetName ${meta.stitched_dataset}" : ''
    """
    # Create command line parameters
    declare -a app_args
    for file in ${meta.stitching_dir}/*-n5-final.json
    do
        app_args+=( -i "\$file" )
    done
    echo "Fuse args: \${app_args[@]}"
    if [[ ${meta.id} == "t3" ]] ; then
        echo "!!!!! THIS IS TESTING"
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
        exit 0
    fi
    /opt/scripts/runapp.sh "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar org.janelia.stitching.StitchingSpark \
        ${spark.parallelism} ${spark.worker_cores} "${executor_memory}" ${spark.driver_cores} "${driver_memory}" \
        --fuse \${app_args[@]} \
        ${darkfield_file_arg} \
        ${flatfield_file_arg} \
        ${stitching_result_dir_arg} \
        ${stitched_container_arg} \
        ${stitched_dataset_arg} \
        ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """

    stub:
    """
    # Create final output
    mkdir -p ${meta.stitching_dir}/export.n5
    echo "{\"n5\":\"2.2.0\"}" > ${meta.stitching_dir}/export.n5/attributes.json

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
