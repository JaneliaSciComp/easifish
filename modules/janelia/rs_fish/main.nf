process RS_FISH {
    tag "${meta.id}"
    container "ghcr.io/janeliascicomp/rs-fish-spark:8f8954f"
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta),
          path(input_image),
          val(input_dataset),
          path(spots_output_dir, stageAs: 'spots/*'),
          val(spark)

    output:
    tuple val(meta),
          path(input_image),
          val(input_dataset),
          path(spots_output_dir),
          val(spark),                           emit: params
    tuple val(meta), env(full_output_filename), emit: csv
    path "versions.yml",                        emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def output_filename = "${meta.id}-points.csv"
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    """
    INPUT_N5=\$(realpath ${input_image})
    full_spots_dir=\$(readlink -m ${spots_output_dir})
    mkdir -p \${full_spots_dir}
    full_output_filename=\${full_spots_dir}/${output_filename}
    /opt/scripts/runapp.sh "$workflow.containerEngine" "$spark.work_dir" "$spark.uri" \
        /app/app.jar net.preibisch.rsfish.spark.SparkRSFISH \
        $spark.parallelism $spark.worker_cores "$executor_memory" $spark.driver_cores "$driver_memory" \
        --image=\$INPUT_N5 --dataset=${input_dataset} \
        --output=\${full_output_filename} \
        ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        rs-fish-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
