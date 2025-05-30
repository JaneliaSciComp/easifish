process LINK {
    tag "${samplesheet_row.filename}"
    label 'process_single'
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/stitching-spark:1.11.0' }

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
    def filename = file(samplesheet_row.filename).name
    """
    input_fullpath=\$(readlink -e ${input_dir})
    output_fullpath=\$(readlink -m "${output_dir}/${samplesheet_row.id}")
    full_filename="\${input_fullpath}/${filename}"
    if [[ ! -e \${output_fullpath} ]] ; then
        echo "Create output directory: \${output_fullpath}"
        mkdir -p \${output_fullpath}
    else
        echo "Output directory: \${output_fullpath} - already exists"
    fi
    pushd \${output_fullpath}
    echo "Create link \${output_fullpath}/${filename} to \${full_filename}"
    ln -sf \${full_filename} ${filename}
    popd

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        ln: \$(ln --version | sed 's/ln (GNU coreutils) //g')
    END_VERSIONS
    """
}
