//  modify asdf->data collection to bulk change base paths

db.data.find({}, {base:1}).forEach(function(doc){
    base = doc['base']
    old_root = '/sciclone/aiddata10/REU/data'
    new_root = '/sciclone/aiddata10/REU/geo/data'
    if (base.startsWith(old_root)) {
        new_base = base.replace(old_root, new_root)
        print(new_base)
    }
})
