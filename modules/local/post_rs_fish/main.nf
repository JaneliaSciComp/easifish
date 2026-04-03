process POST_RS_FISH {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/easifish-spots-utils:v1.2-ome-dask2025.11.0'

    input:
    tuple val(meta),
          path(input_path),
          val(input_dataset),
          path(voxel_spots_csv_file),
          val(spots_image_subpath_ref)

    output:
    tuple val(meta),
          env('full_input_path'),
          val(input_dataset),
          env('spots_results')  , emit: results
    path "versions.yml"         , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def rsfish_spots_filename = file(voxel_spots_csv_file).name
    def spots_filename = rsfish_spots_filename.replace('rsfish-', '').replace('.csv', '-coord.csv')
    def spots_filepattern = rsfish_spots_filename.replace('rsfish-', '').replace('.csv', '-coord*.csv')

    """
    # Create command line parameters
    if [[ ! -e ${input_path} ]]; then
        # when post-rs-fish is actually skipped because the data is missing
        # this should not error out
        echo "Input path ${input_path} not found"
        full_input_path=
        spots_results=
    else
        full_input_path=\$(readlink -e ${input_path})
        echo "Input volume: \${full_input_path}"
        full_voxel_spots_csv_file=\$(readlink ${voxel_spots_csv_file})
        echo "Input spots CSV file: \${full_voxel_spots_csv_file}"

        if [[ -f \${full_voxel_spots_csv_file} ]]; then
            echo "Found voxel spots CSV file: \${full_voxel_spots_csv_file}"
            voxel_spots_csv_dir=\$(dirname \${full_voxel_spots_csv_file})
            coord_spots_csv_file=\${voxel_spots_csv_dir}/${spots_filename}

            CMD=(
                python -m easifish_spots_tools.post_rs_fish
                --image-container \${full_input_path}
                --image-subpath ${input_dataset}
                --input \${full_voxel_spots_csv_file}
                --output \${coord_spots_csv_file}
                ${args}
            )
            echo "CMD: \${CMD[@]}"
            (exec "\${CMD[@]}")

            spots_results=\$(ls \${voxel_spots_csv_dir}/${spots_filepattern})
            echo "Spots results \${spots_results}"
        else
            echo "Voxel spots CSV file not found: \${full_voxel_spots_csv_file}"
            spots_results=
        fi
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        post-rs-fish: v1
    END_VERSIONS
    """
}
