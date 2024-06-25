process MULTISCALE_PYRAMID {
    tag { meta.id }
    container 'docker.io/janeliascicomp/n5-tools-spark:9097071'
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
    for scale in \$(seq 1 20); do
        tmp_scale_subpath = \$(echo "$fullscale_dataset" | sed s/s0$/s\$scale")
        prev_scale_subpath = \$(echo "$fullscale_dataset" | sed s/s0$/prev-s\$scale")
        echo "Check \$tmp_scale_subpath"
        if [[ -d "\${full_n5_container_path}/\${tmp_scale_subpath}" ]] ; then
            echo "mv \${full_n5_container_path}/\${tmp_scale_subpath} \${full_n5_container_path}/\${prev_scale_subpath}"
            mv "\${full_n5_container_path}/\${tmp_scale_subpath}" "\${full_n5_container_path}/\${prev_scale_subpath}"
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