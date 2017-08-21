//  modify asdf->data collection to bulk change base paths

db.data.find({}).forEach(function(doc){

    base = doc['base']

    old_root = '/sciclone/aiddata10/REU/data'
    new_root = '/sciclone/aiddata10/REU/geo/data'

    new_base = base.replace(old_root, new_root)

    final_base = new_base.replace('external/global/', '')
    // print(final_base)

    doc['base'] = final_base
    db.data.save(doc)

})



db.data.find({}, {base:1}).forEach(function(doc){

    base = doc['base']
    print(base)

})
