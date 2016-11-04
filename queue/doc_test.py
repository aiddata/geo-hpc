

import pymongo
from bson.objectid import ObjectId
client = pymongo.MongoClient('128.239.20.76')
# request = client.asdf.det.find_one({'_id': ObjectId('57a2ff1bc15e002f448b4568')})


request = {
    "_id" : ObjectId("57a2ff1bc15e002f448b4568"),
    "boundary" : {
        "title" : "Nigeria ADM0 Boundary - GADM 2.8",
        "group" : "nga_gadm28",
        "name" : "nga_adm0_gadm28",
        "description" : "GADM Boundary File for ADM0 (Country) in Nigeria.",
        "path" : "/sciclone/aiddata10/REU/data/boundaries/gadm2.8/NGA_adm0NGA_adm0.geojson"
    },
    "release_data" : [
        {
            "dataset" : "nigeriaaims_geocodedresearchrelease_level1_v1_3",
            "custom_name" : "New Request",
            "filters" : {
                "donors" : [
                    "AFDB"
                ],
                "ad_sector_names" : [
                    "All"
                ]
            },
            "$$hashKey" : "object:2626"
        },
        {
            "dataset" : "nigeriaaims_geocodedresearchrelease_level1_v1_3",
            "custom_name" : "hiiiiii",
            "filters" : {
                "donors" : [
                    "AFDB",
                    "France"
                ],
                "ad_sector_names" : [
                    "All"
                ]
            },
            "$$hashKey" : "object:3400"
        }
    ],
    "raster_data" : [
        {
            "name" : "udel_precip_v401_min",
            "title" : "UDel Precipitation (v4.01) Yearly Aggregate (min)",
            "base" : "/sciclone/aiddata10/REU/data/rasters/external/global/udel_climate/precip_2014_v4.01/yearly/min",
            "type" : "raster",
            "custom_name" : "fo",
            "temportal_type" : "year",
            "options" : {
                "extract_types" : [
                    "mean"
                ]
            },
            "files" : [
                {
                    "name" : "udel_precip_v401_min_1998",
                    "path" : "precip_1998_min.tif",
                    "$$hashKey" : "object:2545"
                }
            ],
            "$$hashKey" : "object:2636",
            "temporal_type" : "year"
        }
    ],
    "email" : "eslivinski@gmail.com",
    "custom_name" : "My Full test request",
    "status" : 1,
    "priority" : 0,
    "stage" : [
        {
            "name" : "submit",
            "time" : 1470299931
        },
        {
            "name" : "prep",
            "time" : 1470419931
        },
        {
            "name" : "process",
            "time" : 1470419931
        },
        {
            "name" : "complete",
            "time" : 1470419933
        }
    ]
}


from documentation_tool import DocBuilder
doc = DocBuilder(client, request, '/sciclone/aiddata10/REU/test/doc_test.pdf')
doc.build_doc()

