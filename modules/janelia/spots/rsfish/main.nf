process SPOTS_RSFISH {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/rs-fish-spark:8f8954f'
    cpus { spark.driver_cpus }
    memory { "${spark.driver_memory as int}g" }

    input:
    tuple val(meta),
          path(input_image),
          val(input_dataset),
          path(spots_output_dir, stageAs: 'spots/*'),
          val(spots_result_name),
          val(spots_channels),
          val(spark)

    output:
    tuple val(meta),
          env('INPUT_IMG'),
          val(input_dataset),
          path(spots_output_dir),
          val(spots_result_name),
          val(spark),                             emit: params
    tuple val(meta), env('full_output_filename'), emit: csv
    path "versions.yml",                          emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def output_filename = spots_result_name ?: "${meta.id}-points.csv"
    def spots_channels_arg = spots_channels ? "--included-channels=${spots_channels}" : ''
    def executor_memory_gb = spark.executor_memory as int
    def driver_memory_gb = spark.driver_memory as int
    """
    case \$(uname) in
        Darwin)
            detected_os=OSX
            READLINK_TOOL="greadlink"
            ;;
        *)
            detected_os=Linux
            READLINK_TOOL="readlink"
            ;;
    esac
    INPUT_IMG=\$(realpath ${input_image})
    full_spots_dir=\$(\${READLINK_TOOL} -m ${spots_output_dir})
    mkdir -p \${full_spots_dir}
    export full_output_filename=\${full_spots_dir}/${output_filename}
    CMD=(
        /opt/scripts/runapp.sh
        "${workflow.containerEngine}"
        "${spark.work_dir}"
        "${spark.uri}"
        /app/app.jar
        net.preibisch.rsfish.spark.SparkRSFISH
        ${spark.parallelism}
        ${spark.executor_cpus}
        "${executor_memory_gb}g"
        ${spark.driver_cpus}
        "${driver_memory_gb}g"
        --spark-conf "spark.jars.ivy=/tmp/.ivy2"
        --spark-conf "spark.driver.extraClassPath=/app/app.jar"
        --spark-conf "spark.executor.extraClassPath=/app/app.jar"
        --spark-conf "spark.driver.extraJavaOptions=-Dnative.libpath.verbose=true"
        --image=\${INPUT_IMG}
        --dataset=${input_dataset}
        --output=\${full_output_filename}
        ${spots_channels_arg}
        ${extra_args}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        rs-fish-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
