process POST_RS_FISH {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/easifish-spots-utils:v1.1-ome' }

    input:
    tuple val(meta),
          path(input_path),
          val(input_dataset),
          path(voxel_spots_csv_file)

    output:
    tuple val(meta),
          env(full_input_path),
          val(input_dataset),
          env(spots_results)  , emit: results
    path "versions.yml"       , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def rsfish_spots_filename = file(voxel_spots_csv_file).name
    def spots_filename = rsfish_spots_filename.replace('rsfish-', '').replace('.csv', '-coord.csv')
    def spots_filepattern = rsfish_spots_filename.replace('rsfish-', '').replace('.csv', '-coord*.csv')

    """
    # Create command line parameters
    full_input_path=\$(readlink -e ${input_path})
    echo "Input volume: \${full_input_path}"
    full_voxel_spots_csv_file=\$(readlink ${voxel_spots_csv_file})
    echo "Input spots CSV file: \${full_voxel_spots_csv_file}"

    if [[ -f \${full_voxel_spots_csv_file} ]]; then
        echo "Found voxel spots CSV file: \${full_voxel_spots_csv_file}"
        voxel_spots_csv_dir=\$(dirname \${full_voxel_spots_csv_file})
        coord_spots_csv_file=\${voxel_spots_csv_dir}/${spots_filename}

        python /opt/scripts/spots-utils/post-rs-fish.py \
            --image-container \${full_input_path} \
            --image-subpath ${input_dataset} \
            --input \${full_voxel_spots_csv_file} \
            --output \${coord_spots_csv_file}

        spots_results=\$(ls \${voxel_spots_csv_dir}/${spots_filepattern})
        echo "Spots results \${spots_results}"
    else
        echo "Voxel spots CSV file not found: \${full_voxel_spots_csv_file}"
        spots_results=
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        post-rs-fish: v1
    END_VERSIONS
    """
}
