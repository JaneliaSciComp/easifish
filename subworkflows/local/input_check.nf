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

    main:
    SAMPLESHEET_CHECK(samplesheet)
        .csv
        .splitCsv ( header:true, sep:',' )
        .branch {
            row ->
                remote: row.containsKey('uri')
                    return row
                local: true
                    return row
    }
    .set { tiles }

    def downloaded_tiles = DOWNLOAD(tiles.remote, output_image_dir).tiles
    def linked_tiles = LINK(tiles.local, input_image_dir, output_image_dir).tiles
    // download remote tiles
    downloaded_tiles
        .mix(linked_tiles)
        .map { row, image_dir ->
            create_acq_channel(row, image_dir)
        }
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
    def filepath = "${image_dir}/${image_name}"
    return [meta, file(filepath), samplesheet_row.pattern]
}
