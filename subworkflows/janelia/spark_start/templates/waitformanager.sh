#!/bin/bash -ue
# Watch the given Spark manager log and wait for it to be accessible, then
# capture the URI so the process can emit it as env('spark_uri').

set +x

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})
spark_master_log_file="\${full_spark_work_dir}/sparkmaster.log"
terminate_file_name="\${full_spark_work_dir}/terminate-spark-${workflow.sessionId}"
sleep_secs="${task.ext.sleep_secs ?: '1'}"
max_wait_secs="${task.ext.max_wait_secs ?: '3600'}"

elapsed_secs=0
while true; do

    if [[ -e \${spark_master_log_file} ]]; then
        test_uri=\$(grep -o "\\(spark://.*\$\\)" \${spark_master_log_file} || true)
        if [[ ! -z \${test_uri} ]]; then
            echo "Spark master started at \${test_uri}"
            break
        fi
    fi

    if [[ -e "\${terminate_file_name}" ]]; then
        echo "Terminate file \${terminate_file_name} found"
        exit 1
    fi

    if (( \${elapsed_secs} > \${max_wait_secs} )); then
        echo "Timed out after \${elapsed_secs} seconds while waiting for Spark master <- \${spark_master_log_file}"
        cat \${spark_master_log_file} >&2
        exit 2
    fi

    sleep \${sleep_secs}
    elapsed_secs=\$(( \${elapsed_secs} + \${sleep_secs} ))
done

echo \${test_uri} > spark_uri
spark_uri=\$(cat spark_uri)
echo "Export spark URI: \${spark_uri}"
