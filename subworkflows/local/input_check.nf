//
// Check input samplesheet and download images if necessary
//

include { SAMPLESHEET_CHECK } from '../../modules/local/samplesheet_check'
include { DOWNLOAD          } from '../../modules/local/download/main'
include { LINK              } from '../../modules/local/link/main'

workflow INPUT_CHECK {
    take:
    samplesheet // file: /path/to/samplesheet.csv
    input_image_dir
    output_image_dir
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
    if (skip) {
        // take the tiles and prepare the output as if it was downloaded
        prepare_acq = tiles.remote
            .mix(tiles.local)
            .map { row ->
                log.debug "Skipped downloading ${row}"
                create_acq_channel(row, output_image_dir)
            }
    } else {
        // download remote tiles
        def downloaded_tiles = DOWNLOAD(tiles.remote, output_image_dir).tiles
        def linked_tiles = LINK(tiles.local, input_image_dir, output_image_dir).tiles

        prepare_acq = downloaded_tiles
            .mix(linked_tiles)
            .map { row, image_dir ->
                log.debug "Completed download/link for ${row}"
                create_acq_channel(row, image_dir)
            }
    }
    prepare_acq
        .groupTuple() // Group by acquisition
        .map {
            def (meta, files, patterns) = it
            def trimmed_patterns = patterns.findAll { it?.trim() }
            if (trimmed_patterns) {
                // Set acquisition's filename pattern to the meta map
                meta.pattern = trimmed_patterns.first()
            }
            // Set image dir to the meta map
            meta.image_dir = files.collect { file(it).parent }.first()
            def r = [meta, files]
            log.debug "Set acquisitions $it -> $r"
            r
        }
        .set { acquisitions }

    emit:
    acquisitions                              // channel: [ val(meta), [ filenames ] ]
    versions = SAMPLESHEET_CHECK.out.versions // channel: [ versions.yml ]
}

def create_acq_channel(LinkedHashMap samplesheet_row, image_dir) {
    def meta = [:]
    def image_name = file(samplesheet_row.filename).name
    meta.id = samplesheet_row.id
    if (samplesheet_row.warped_channels_map) {
        meta.warped_channels_mapping = extract_warped_channels_mapping(samplesheet_row.warped_channels_map)
    }
    def filepath = "${image_dir}/${image_name}"
    return [meta, file(filepath), samplesheet_row.pattern]
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
