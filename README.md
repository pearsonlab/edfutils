# edfutils
Tools for processing edf files

## edf_split
This script will take in an edf file and split it into separate .chn files for each channel and chunk (you specify a chunk size, otherwise it defaults to 60). These .chn files include the following in a json header:

* 'read_instruct': Instructions on how to read the channel files
* 'sigLabel': The label of the signal from the original .edf. This is redundant from the filename of the .chn file.
* 'start_time': The start time of the chunk.
* 'chunkTime': Time of the chunk in minutes (user-specified).
* 'filename': Name of the original file.
* 'sampsPerRecord': Number of samples in each data record.
* 'chunk': The chunk number (these are in order)
* 'recsPerChunk': Number of records in each chunk.

**NOTE**: For the last chunk, the number of records will be different from recsPerChunk.