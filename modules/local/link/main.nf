process LINK {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/stitching-spark:1.11.0-rc2'

    input:
    val samplesheet_row
    path(input_dir, stageAs: 'in/*')
    path(output_dir, stageAs: 'out/*')

    output:
    tuple val(samplesheet_row), env('input_fullpath'), env('output_fullpath'), emit: tiles
    path "versions.yml"                                                      , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in ./bin
    """
    input_fullpath=\$(readlink -e ${input_dir})
    output_fullpath=\$(readlink -m "${output_dir}/${samplesheet_row.id}")
    full_filepath="\${input_fullpath}/${samplesheet_row.filename}"
    if [[ ! -e \${output_fullpath} ]] ; then
        echo "Create output directory: \${output_fullpath}"
        mkdir -p \${output_fullpath}
    else
        echo "Output directory: \${output_fullpath} - already exists"
    fi

    if [[ ! -s "\${output_fullpath}/${samplesheet_row.filename}" ]]; then
        pushd \${output_fullpath}
        echo "Create link \${output_fullpath}/${samplesheet_row.filename} to \${full_filepath}"
        ln -sf \${full_filepath} ${samplesheet_row.filename}
        popd
    else
        echo "No links were needed"
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        ln: \$(ln --version | sed 's/ln (GNU coreutils) //g')
    END_VERSIONS
    """
}
