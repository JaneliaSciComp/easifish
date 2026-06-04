#!/bin/bash -ue
# Remove staged app jars from the spark working directory.

case \$(uname) in
    Darwin) READLINK_TOOL="greadlink" ;;
    *)      READLINK_TOOL="readlink"  ;;
esac
full_spark_work_dir=\$(\${READLINK_TOOL} -m ${spark_work_dir})

find \${full_spark_work_dir} -name app.jar -exec rm {} \\;
