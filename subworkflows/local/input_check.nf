//
// Check input samplesheet and download images if necessary
//

include { SAMPLESHEET_CHECK } from '../../modules/local/samplesheet_check'
include { DOWNLOAD } from '../../modules/local/download'

workflow INPUT_CHECK {
    take:
    image_dir
    samplesheet // file: /path/to/samplesheet.csv

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

    DOWNLOAD(image_dir, tiles.remote)
        .tiles
        .mix(tiles.local)
        .map { create_acq_channel(it, image_dir) }
        // Group by acquisition
        .groupTuple()
        .map {
            def (meta, files, patterns) = it
            // Set acquisition's filename pattern to the meta map
            meta.pattern = patterns.findAll({ it?.trim() }).first()
            // Set image dir to the meta map
            meta.image_dir = image_dir
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
    meta.id = samplesheet_row.id
    filepath = "${image_dir}/${samplesheet_row.filename}"
    return [meta, file(filepath), samplesheet_row.pattern]
}
