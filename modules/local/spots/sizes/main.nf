process SPOTS_SIZES {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/easifish-spots-utils:v1' }
    cpus { ncpus }
    memory { "${mem_in_gb}GB" }

    input:
    tuple val(meta),
          path(input_image_path),
          val(input_dataset),
          path(labels_path),
          val(labels_dataset),
          path(spots_input_dir),
          val(input_pattern),
          path(output_dir)
    val(ncpus)
    val(mem_in_gb)

    output:
    tuple val(meta),
          env(full_input_image_path),
          val(input_dataset),
          env(full_spots_input_dir),
          env(output_csv_file),      emit: results
    path "versions.yml"            , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    full_input_image_path=\$(readlink -e ${input_image_path})
    full_labels_path=\$(readlink -e ${labels_path})
    full_spots_input_dir=\$(readlink -e ${spots_input_dir})
    full_output_dir=\$(readlink ${output_dir})

    mkdir -p \${full_output_dir}

    output_csv_file="\${full_output_dir}/count.csv"

    python /opt/scripts/spots-utils/labeled-spots-sizes.py \
        --image-container \${full_input_image_path} \
        --image-subpath ${input_dataset} \
        --labels-container \${full_labels_path} \
        --labels-subpath ${labels_dataset} \
        --spots-pattern "\"\${full_spots_input_dir}/${input_pattern}\"" \
        --output \${output_csv_file}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        assign-spots: v1
    END_VERSIONS
    """

}
