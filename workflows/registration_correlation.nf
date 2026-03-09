include { BIGSTREAM_CORRELATIONMETRIC                              } from '../modules/janelia/bigstream/correlationmetric/main'

workflow REGISTRATION_CORRELATION {
    take:
    ch_inputs  // [reg_meta,
               //  fix_image, fix_dataset,
               //  fix_timeindex, fix_channel
               //  mov_image, mov_dataset,
               //  mov_timeindex, mov_channel,
               //  output_container, output_dataset]

    main:
    BIGSTREAM_CORRELATIONMETRIC(
        ch_inputs
        | map { it ->
            def (reg_meta,
                 fix_image, fix_dataset,
                 fix_timeindex, fix_channel,
                 mov_image, mov_dataset,
                 mov_timeindex, mov_channel,
                 output_container, output_dataset) = it
            [
                reg_meta,
                fix_image, fix_dataset,
                fix_timeindex, fix_channel,
                '',   // fix_spacing
                mov_image, mov_dataset,
                mov_timeindex, mov_channel,
                '',   // mov_spacing
                output_container, output_dataset,
            ]
        }
    )
}
