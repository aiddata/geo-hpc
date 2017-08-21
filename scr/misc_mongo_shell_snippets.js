// update asdf.data collection for all rasters
// add extract_types_info dict based on extract_types list
// just added placeholder text as dict vals
db.data.find({type:'raster'}).forEach( function (doc) {
    var extract_types_info = {};
    doc['options']['extract_types'].forEach( function (type) {
        extract_types_info[type] = 'text about ' + type + ' extract method for ' + doc['name'];
    });
    print(extract_types_info);
    doc['options']['extract_types_info'] = extract_types_info;
    db.data.save(doc);
})


// iterate over tracker collections
// remove and recreate single index
use trackers
db.getCollectionNames().forEach( function (cname) {
    // print(cname);
    db[cname].dropIndex({name:1});
    db[cname].createIndex({name:1});

})


// iterate over tracker collections
// to reset status of subset of docs
use trackers
db.getCollectionNames().forEach( function (cname) {
    db[cname].update({type: 'release'}, {$set: {status:-1}}, {multi:1})
})



// sum of times
// (this was supposed to be for extracts,
// but we don't log runtime for extracts
// only submit/update timestamp)
db.extracts.aggregate([
    {
        $match: {version: '0.3.1', status: 1}
    },
    {
        $group: {_id: null, sum: {$sum: "complete_time"}}
    }

])


var max = 0;
db.test.find().forEach(function(obj) {
    var curr = Object.bsonsize(obj);
    if(max < curr) {
        max = curr;
    }
})
print(max)




var count = 0;
db.features.find().forEach(function(obj) {
    var curr = Object.bsonsize(obj);
    if(curr > 12000000) {
        count = count + 1;
        print(obj['datasets'][0]);
    }
})
print(count)


