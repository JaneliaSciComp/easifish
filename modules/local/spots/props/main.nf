process SPOTS_PROPS {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/easifish-spots-utils:v1.1-ome' }
    cpus { ncpus }
    memory { "${mem_in_gb}GB" }

    input:
    tuple val(meta),
          path(input_image_path, stageAs: 'image/*'),
          val(input_dataset),
          path(labels_path, stageAs: 'labels/*'),
          val(labels_dataset),
          val(dapi_dataset),
          val(bleed_dataset),
          path(output_dir, stageAs: 'output/*'),
          val(output_name)
    val(ncpus)
    val(mem_in_gb)

    output:
    tuple val(meta),
          env(full_input_image_path),
          val(input_dataset),
          env(output_csv_file)      , emit: results
    path "versions.yml"             , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    dapi_dataset_arg = dapi_dataset ? "--dapi-subpath ${dapi_dataset}" : ""
    bleed_dataset_arg = bleed_dataset ? "--bleed-subpath ${bleed_dataset}" : ""
    """
    full_input_image_path=\$(readlink -e ${input_image_path})
    full_labels_path=\$(readlink -e ${labels_path})
    full_output_dir=\$(readlink ${output_dir})

    mkdir -p \${full_output_dir}

    output_csv_file="\${full_output_dir}/${output_name}"

    CMD=(
        python
        /opt/scripts/spots-utils/labeled-spots-props.py
        --image-container \${full_input_image_path}
        --image-subpath ${input_dataset}
        --labels-container \${full_labels_path}
        --labels-subpath ${labels_dataset}
        ${dapi_dataset_arg}
        ${bleed_dataset_arg}
        --output \${output_csv_file}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        measure-spots: v1
    END_VERSIONS
    """

}
