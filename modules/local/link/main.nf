process LINK {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0' }

    input:
    val samplesheet_row
    path(input_dir, stageAs: 'in/*')
    path(output_dir, stageAs: 'out/*')

    output:
    tuple val(samplesheet_row), env(input_fullpath), env(output_datalink), emit: tiles
    path "versions.yml"                                                  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in ./bin
    """
    input_fullpath=\$(readlink -e ${input_dir})
    output_fullpath=\$(readlink -m "${output_dir}")
    full_filepath="\${input_fullpath}/${samplesheet_row.filename}"
    full_parentpath=\$(dirname \${full_filepath})
    if [[ ! -e \${output_fullpath} ]] ; then
        echo "Create output directory: \${output_fullpath}"
        mkdir -p \${output_fullpath}
    else
        echo "Output directory: \${output_fullpath} - already exists"
    fi

    if [[ "\${output_fullpath}/${samplesheet_row.id}" != "\${full_parentpath}" ]]; then
        pushd \${output_fullpath}
        echo "Create link \${output_fullpath}/${samplesheet_row.id} to \${full_filepath}"
        ln -sf \${full_parentpath} ${samplesheet_row.id}
        popd
    else
        echo "No links created because \${output_fullpath} is the same as input"
    fi
    output_datalink="\${output_fullpath}/${samplesheet_row.id}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        ln: \$(ln --version | sed 's/ln (GNU coreutils) //g')
    END_VERSIONS
    """
}
