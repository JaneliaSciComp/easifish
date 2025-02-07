def as_list(v) {
    def vlist
    if (v instanceof Collection) {
        vlist = deformation_entries
    } else if (v) {
        vlist = v.tokenize(',').collect { it.trim() }
    } else {
        vlist = []
    }
    vlist
}
