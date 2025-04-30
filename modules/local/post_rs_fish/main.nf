process POST_RS_FISH {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/easifish-spots-utils:v1' }

    input:
    tuple val(meta),
          path(input_path),
          val(input_dataset),
          val(voxel_spots_csv_file)

    output:
    tuple val(meta),
          env(full_input_path),
          val(input_dataset),
          env(coord_spots_csv_file), emit: results
    path "versions.yml"            , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def rsfish_spots_filename = file(voxel_spots_csv_file).name
    def spots_filename = rsfish_spots_filename.replace('rsfish-', '').replace('.csv', '-coord.csv')

    """
    # Create command line parameters
    full_input_path=\$(readlink -e ${input_path})
    full_voxel_spots_csv_file=\$(readlink ${voxel_spots_csv_file})

    if [[ -f \${full_voxel_spots_csv_file} ]]; then
        echo "Voxel spots CSV file not found: \${full_voxel_spots_csv_file}"
        coord_spots_csv_file=
    else
        voxel_spots_csv_dir=\$(dirname \${full_voxel_spots_csv_file})
        coord_spots_csv_file=\${voxel_spots_csv_dir}/${spots_filename}

        python /opt/scripts/spots-utils/post-rs-fish.py \
            --image-container \${full_input_path} \
            --image-subpath ${input_dataset} \
            --input \${full_voxel_spots_csv_file} \
            --output \${coord_spots_csv_file}
    fi


    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        post-rs-fish: v1
    END_VERSIONS
    """

}
