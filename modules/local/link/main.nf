process LINK {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container "ghcr.io/janeliascicomp/stitching-spark:1.11.0"

    input:
    val samplesheet_row
    path(input_dir, stageAs: 'in/*')
    path(output_dir, stageAs: 'out/*')

    output:
    tuple val(samplesheet_row), env(output_fullpath), emit: tiles
    path "versions.yml"                             , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in ./bin
    def full_filename = samplesheet_row.filename[0] == '/'
        ? samplesheet_row.filename
        : "\$(readlink -e ${input_dir})/${samplesheet_row.filename}"
    def filename = file(samplesheet_row.filename).name
    """
    output_fullpath=\$(readlink -m ${output_dir})
    if [[ ! -e \${output_fullpath} ]] ; then
        echo "Create output directory: \${output_fullpath}"
        mkdir -p \${output_fullpath}
    else
        echo "Output directory: \${output_fullpath} - already exists"
    fi
    pushd \${output_fullpath}
    ln -s ${full_filename} ${filename}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        ln: \$(ln --version | sed 's/ln (GNU coreutils) //g')
    END_VERSIONS
    """
}
