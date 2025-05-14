def as_list(v) {
    def vlist
    if (v instanceof Collection) {
        vlist = v
    } else if (v) {
        vlist = v.tokenize(',').collect { it.trim() }
    } else {
        vlist = []
    }
    vlist
}
