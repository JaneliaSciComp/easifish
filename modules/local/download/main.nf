process DOWNLOAD {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2' }

    input:
    val samplesheet_row
    path download_dir

    output:
    tuple val(samplesheet_row), env(download_fullpath), env(download_fullpath), emit: tiles
    path "versions.yml"                                                       , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    // no longer use the download script from bin
    // because if everything is in one place it is easier to debug from the work dir
    """
    download_fullpath=\$(readlink -m "${download_dir}/${samplesheet_row.id}")
    if [[ ! -e \${download_fullpath} ]] ; then
        echo "Create download directory: \${download_fullpath}"
        mkdir -p \${download_fullpath}
    else
        echo "Download directory: \${download_fullpath} - already exists"
    fi

    if [[ ! -e "\${download_fullpath}/${samplesheet_row.filename}" ]]; then
        echo "Download ${samplesheet_row.uri} to \${download_fullpath}/${samplesheet_row.filename}"
        curl -skL --user-agent 'Mozilla/5.0' "${samplesheet_row.uri}" -o "\${download_fullpath}/${samplesheet_row.filename}"
    fi

    if [[ "${samplesheet_row.checksum}" ]]; then
        echo "Verify checksum"
        if md5sum -c <<< "${samplesheet_row.checksum} \${download_fullpath}/${samplesheet_row.filename}"; then
            echo "Checksum verified for \${download_fullpath}/${samplesheet_row.filename}"
        else
            echo "Checksum failed for \${download_fullpath}/${samplesheet_row.filename}"
            exit 1
        fi
    fi

    if [[ \${download_fullpath}/${samplesheet_row.filename} == *.zip ]]; then
        # use dirname in case samplesheet_row.filename has relative paths
        parentdir=\$(dirname \${download_fullpath}/${samplesheet_row.filename})
        unzip -o -d \${parentdir} \${download_fullpath}/${samplesheet_row.filename}
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        download: 1.1.0
    END_VERSIONS
    """
}
