process DOWNLOAD {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0' }

    input:
    val samplesheet_row
    path download_dir

    output:
    tuple val(samplesheet_row), env(download_fullpath), emit: tiles
    path "versions.yml"                               , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in ./bin
    """
    download_fullpath=\$(readlink -m "${download_dir}/${samplesheet_row.id}")
    if [[ ! -e \${download_fullpath} ]] ; then
        echo "Create download directory: \${download_fullpath}"
        mkdir -p \${download_fullpath}
    else
        echo "Download directory: \${download_fullpath} - already exists"
    fi

    download.sh ${samplesheet_row.uri} "\${download_fullpath}/${samplesheet_row.filename}" ${samplesheet_row.checksum}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        download: 1.1.0
    END_VERSIONS
    """
}


