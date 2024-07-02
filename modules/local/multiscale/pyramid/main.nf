process MULTISCALE_PYRAMID {
    tag { meta.id }
    container { task.ext.container ?: 'docker.io/janeliascicomp/n5-tools-spark:9097071' }
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(n5_container), val(fullscale_dataset), val(spark)

    output:
    tuple val(meta), env(full_n5_container_path), val(fullscale_dataset), val(spark), emit: data
    path "versions.yml"                                                             , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')

    """
    # Create command line parameters
    full_n5_container_path=\$(readlink -e ${n5_container})
    echo "Generate pyramid for \${full_n5_container_path}:${fullscale_dataset}"
    # move existing scale levels because the downsampler 
    # fails if a downsample folder already exists
    intermediate_data=\$(echo "$fullscale_dataset" | sed -e s/s0\$/intermediate-downsampling-XY/)
    renamed_intermediate_data=\$(echo "$fullscale_dataset" | sed -e s/s0\$/prev-intermediate-downsampling-XY/)
    if [[ -e "\${full_n5_container_path}/\${intermediate_data}" ]] ; then
        echo "Rename \${intermediate_data} -> \${renamed_intermediate_data}"
        mv -f "\${full_n5_container_path}/\${intermediate_data}" "\${full_n5_container_path}/\${renamed_intermediate_data}"
    fi
    for scale in \$(seq 1 20) ; do
        checked_dataset=\$(echo "$fullscale_dataset" | sed -e s/s0\$/s\${scale}/)
        renamed_dataset=\$(echo "$fullscale_dataset" | sed -e s/s0\$/prev-s\${scale}/)
        echo "Check \${full_n5_container_path}/\$checked_dataset"
        if [[ -e "\${full_n5_container_path}/\${checked_dataset}" ]] ; then
            echo "Rename \${checked_dataset} to \${renamed_dataset} -> mv ${n5_container}/\${checked_dataset} ${n5_container}/\${renamed_dataset}"
            mv -f "\${full_n5_container_path}/\${checked_dataset}" "\${full_n5_container_path}/\${renamed_dataset}"
        fi
    done
    /opt/scripts/runapp.sh "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark \
        ${spark.parallelism} ${spark.worker_cores} "${executor_memory}" ${spark.driver_cores} "${driver_memory}" \
        -n \${full_n5_container_path} -i ${fullscale_dataset} \
        ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        stitching-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """

}