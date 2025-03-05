process POST_RS_FISH {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/post-rs-fish:v1' }

    input:
    tuple val(meta),
          path(input_container),
          val(input_dataset),
          val(voxel_spots_csv_file)

    output:
    tuple val(meta), 
          path(input_container), 
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
    full_input_container_path=\$(readlink -e ${input_container})
    voxel_spots_csv_file=\$(readlink -e ${voxel_spots_csv_file})
    voxel_spots_csv_dir=\$(dirname \${voxel_spots_csv_file})
    coord_spots_csv_file=\${voxel_spots_csv_dir}/${spots_filename}

    python /opt/scripts/post_rs_fish.py \
        --image-container \${full_input_container_path} \
        --image-subpath ${input_dataset} \
        --input \${voxel_spots_csv_file} \
        --output \${coord_spots_csv_file}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        post-rs-fish: v1
    END_VERSIONS
    """

}