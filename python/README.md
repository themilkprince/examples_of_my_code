The attached code takes a geojson that consists of a series of fire objects in the following format:

```
{
"type": "Feature",
"geometry": {
  "type": "Polygon",
  "coordinates": [
                    [
                        [
                            -118.6153,
                            34.0489
                        ],
                        [
                            -118.5717,
                            34.036
                        ],
                        etc...
                    ]
                ]
},
"properties": {
  "mtbs_id": "ID",
  "fire_size": int,
  "date": "YYYY-MM-DD",
  "day_of_fire": int,
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD",
  "fire_size_class": string
  }
}
```
The geometry within this data is then used as a polygon mask for a raster image to mask out those coordinates of the raster and save the results under a file by the name: "mtbs_id_date". 
This code additionally uses Python's Ray library to make the job threaded by batching 20 jobs at a time (the machine this job initially ran on had 20 cores).
