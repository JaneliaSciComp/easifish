//
// Check input samplesheet and download images if necessary
//

include { SAMPLESHEET_CHECK } from '../../modules/local/samplesheet_check'
include { DOWNLOAD          } from '../../modules/local/download/main'
include { LINK              } from '../../modules/local/link/main'

workflow INPUT_CHECK {
    take:
    samplesheet          // ch: /path/to/samplesheet.csv
    ch_input             // ch: /path/to/input_data
    output_image_dir     // String|file: path to output
    skip

    main:
    SAMPLESHEET_CHECK(samplesheet)
        .csv
        .splitCsv ( header:true, sep:',' )
        .branch {
            row ->
                remote: row.containsKey('uri')
                        log.debug "Remote tile: $row"
                        return row
                local: true
                       log.debug "Local tile: $row"
                       return row
    }
    .set { tiles }

    def prepare_acq
    def ch_input_image = ch_input

    if (skip) {
        // take the tiles and prepare the output as if it was downloaded
        prepare_acq = tiles.remote
            .mix(tiles.local)
            .combine(ch_input_image)
            .map { row, input_image_dir ->
                log.debug "Skipped downloading ${row}"
                create_acq_channel(row, input_image_dir, output_image_dir)
            }
    } else {
        // download remote tiles
        def downloaded_tiles = DOWNLOAD(
            tiles.remote,
            output_image_dir
        ).tiles
        def link_inputs = tiles.local
                .combine(ch_input_image)
                .map { row, input_image_dir ->
                    log.debug "Prepare link inputs: ${row}, ${input_image_dir}, ${output_image_dir}"
                    return [ row, input_image_dir, output_image_dir ]
                }
        def linked_tiles = LINK(
            link_inputs.map { it[0] }, // row
            link_inputs.map { it[1] }, // image_dir
            link_inputs.map { it[2] }, // output_dir
        ).tiles

        prepare_acq = downloaded_tiles
            .mix(linked_tiles)
            .map { row, input_dir, image_dir ->
                log.debug "Completed download/link for ${row}"
                create_acq_channel(row, input_dir, image_dir)
            }
    }

    prepare_acq
        .groupTuple() // Group by acquisition
        .map {
            def (id, metas, input_dirs, data_files, patterns) = it
            log.debug "Prepare acquisitions: $it"
            def trimmed_patterns = patterns.findAll { it?.trim() }
            def meta = metas.inject([id:id]) { acc, m -> acc << m }

            if (trimmed_patterns) {
                // Set acquisition's filename pattern to the meta map
                meta.pattern = trimmed_patterns.first()
            }
            def r = [meta, data_files + input_dirs]
            log.debug "Set acquisitions: $r"
            r
        }
        .set { acquisitions }

    emit:
    acquisitions                              // channel: [ val(meta), [ filenames ] ]
    versions = SAMPLESHEET_CHECK.out.versions // channel: [ versions.yml ]
}

def create_acq_channel(LinkedHashMap samplesheet_row, input_dir, image_dir) {
    log.debug "Create acquisition data: from: ${samplesheet_row} , ${input_dir}, ${image_dir}"
    def meta = [:]
    def image_name = file(samplesheet_row.filename).name
    def id = samplesheet_row.id
    if (samplesheet_row.warped_channels_map) {
        meta.warped_channels_mapping = extract_warped_channels_mapping(samplesheet_row.warped_channels_map)
    }
    def filepath = file("${image_dir}/${image_name}")
    meta.image_dir = filepath.parent
    return [id, meta, file(input_dir), file(filepath), samplesheet_row.pattern]
}

def extract_warped_channels_mapping(warped_channels_map) {
    warped_channels_map.tokenize(';')
        .findAll { it.trim() }
        .collect { schannel_mapping ->
            def channel_mapping = schannel_mapping.trim().tokenize(':')
            if (channel_mapping.size == 1) {
                [
                    ("${channel_mapping[0].trim()}" as String) : channel_mapping[0].trim()
                ]
            } else {
                [
                    ("${channel_mapping[0].trim()}" as String) : channel_mapping[1].trim()
                ]
            }
        }
        .collectEntries { it }
}
