# Mordecai Batch
Make sure the geonames index is in this directory and that the elasticsearch container is running (per the mordecai instructions).

` docker run -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2`

then

`pip install -r requirements.txt`

then

python3 mordbatch.py

It reads files from the data dir, fills in the geo data, and writes to the output dir.

## Data Set
Get a collection of 92K CNN news stories from here:

https://cs.nyu.edu/~kcho/DMQA/
