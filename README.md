The Python-AWS folder was created for AWS Cloud Automation with Python.

Bucket

Create bucket python main.py bucket "any-name-for-s3" -cb

Create bucket and Enable Versining python main.py bucket "bucket-with-vers-2" -cb -vers True

Organize bucket per extensions python main.py bucket "bucket-with-vers" -o_b

Object Upload local object from /static folder.

python main.py object "bucket-with-vers" --local_object "important.txt" --upload_type "upload_file"

Upload object link.

python main.py object bucket_name "new-bucket-btu-7" -ol "http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4" -du

List object versions

python main.py object "important.txt" "bucket-with-vers" -l_v

Rollback to version

python main.py object "important.txt" "bucket-with-vers" -r_b_t "En8tj6pxH3nduvOzGpEs5RP5QN6M5UQ6"