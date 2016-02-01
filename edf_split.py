#!/usr/bin/env python
# run "ln -s <this-script's-location> /usr/local/bin/edf_split" to make this script executable from anywhere
import argparse
from urlparse import urlparse
import os
import sys
import boto3
from boto3.s3.transfer import S3Transfer
import threading
import shutil
import tempfile

class Progress(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            relprogress = int((self._seen_so_far / self._size) * 50)
            sys.stdout.write("\r[" + "=" * relprogress + " " * (50 - relprogress) + "]" + str(relprogress*2) + "%")


def local_and_s3_writer(edf, header, local, s3):
    # write locally
    localpaths = local_writer(edf, header, local, s3)

    # connect to s3
    try:
        transfer = S3Transfer(boto3.client('s3', 'us-east-1'))
        print "Connected to S3"
    except:
        print "Unable to connect to S3!  Make sure AWS credentials are stored as environment variables."
        return


    # connect to specific s3 bucket after checking path validity
    parsed_s3 = urlparse(s3)
    if parsed_s3.scheme != 's3':
        print "Must provide valid S3 URI! (starts with 's3://')"
        return

    print "Uploading %i files:" % len(localpaths)
    i = 1
    for path in localpaths:
        if parsed_s3.path != '/': # if path beyond bucket is specified
            fname = os.path.join(parsed_s3.path[1:], path.split('/')[-2], path.split('/')[-1]) # take s3 directory, local folder, and filename
        else:
            fname = os.path.join(path.split('/')[-2], path.split('/')[-1]) # take local folder, and filename
        print "File %i:" % i
        transfer.upload_file(path, parsed_s3.netloc, fname, callback=Progress(path))
        sys.stdout.write('\n')
        i += 1
    print "Upload complete!"
    return

def local_writer(edf, header, local, s3):
    # create directory for byte files
    store_path = os.path.join(local, header['filename'].split('.')[0], '')
    if not os.path.exists(store_path):
        os.makedirs(store_path)

    edf.seek(header['head_length']) # find beginning of data
    files = []
    for i in range(header['numSigs']):
        files.append(open(store_path+header['sigLabels'][i]+'.bin', 'wb')) # create file for each signal

    # make list that marks the indeces of where to cut a data record buffer per signal
    sigBounds = list(header['numSamps'])
    for i in range(header['numSigs']): # mark index where each signal starts and ends within each record
        if i==0:
            sigBounds[i] = tuple((0,sigBounds[i]*2))
        else:
            sigBounds[i] = tuple((sigBounds[i-1][1], sigBounds[i-1][1]+sigBounds[i]*2))

    print "Writing data locally..."
    maxprogress = float((header['numRecs'])*(header['numSigs']))
    # write data from edf to the file
    for i in range(header['numRecs']): # iterate over records
        record = edf.read(sum(header['numSamps'])*2) # read an entire record
        for j in range(header['numSigs']): # iterate over signals within records
            # grab and write signal data from record
            files[j].write(record[sigBounds[j][0]:sigBounds[j][1]])

        # progress bar
        currprogress = float((i+1)*header['numSigs'])
        relprogress = int(50*currprogress/maxprogress)
        sys.stdout.write("\r[" + "=" * relprogress + " " * (50 - relprogress) + "]" +  str(relprogress*2) + "%")

    for f in files:
        f.close() # close files

    print "\nLocal write complete!"

    # create list for all file paths
    filepaths = []
    for i in range(header['numSigs']):
        filepaths.append(store_path+header['sigLabels'][i]+'.bin')

    return filepaths

def s3_writer(edf, header, local, s3):
    try:
        tmp_dir = tempfile.mkdtemp()
        local_and_s3_writer(edf, header, tmp_dir, s3)
    finally:
        shutil.rmtree(tmp_dir)

def head_parser(thisFile):
    header = {}
    header['filename'] = thisFile.name
    # extract info from header
    thisFile.read(176)
    header['start_time'] = tuple(int(i) for i in thisFile.read(8).strip().split('.'))  # makes tup of hour, minute, second
    header['head_length'] = int(thisFile.read(8).strip())
    thisFile.read(44)
    header['numRecs'] = int(thisFile.read(8).strip())
    header['recDur'] = int(thisFile.read(8).strip())
    header['numSigs'] = int(thisFile.read(4).strip())
    header['sigLabels'] = []
    for i in range(header['numSigs']):
        header['sigLabels'].append(thisFile.read(16).strip())
    thisFile.read(header['numSigs']*(80+8+8+8+8+8+80))
    header['numSamps'] = []
    for i in range(header['numSigs']):
        header['numSamps'].append(int(thisFile.read(8).strip()))
    return header

if __name__ == '__main__':
    # set up and parse options
    parser = argparse.ArgumentParser(description='Split edf into files for each channel with proprietary headers. ' +
                                                  'Must specify location for at least one of --local and --s3 flags ' +
                                                  'If only s3 loc is specified, files are written to a temporary ' +
                                                  'directory, which is deleted after the s3 upload is complete.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('edfloc', help='Location of edf file to convert')
    parser.add_argument('--local', help='Location to store folder of binary files (One for each chunk per signal) on the local machine.')
    parser.add_argument('--s3', help='URI formatted location to store binary folder on S3.  Only works if you have AWS ' +
                                    'credentials stored as environment variables.')
    parser.add_argument('--chunk', help='Chunk size (in minutes) to break recordings by', default=60)
    args = parser.parse_args()

    edf_file = args.edfloc
    byte_dir = args.local
    s3_loc = args.s3

    # set up file writer
    if not s3_loc and not byte_dir:
        sys.exit('Must provide an output location (either local (--local), S3 (--s3), or both).')
    elif s3_loc and byte_dir:
        writer = local_and_s3_writer
    elif s3_loc:  # only local directory provided
        writer = s3_writer
    else:
        write = local_writer

    # reads an edf file and splits the signals into a folder of binary files (one for each signal)
    with open(edf_file, 'r+b') as thisFile: # open edf file as read-binary
        # parse header
        header = head_parser(thisFile)

        # write to binary files
        writer(thisFile, header, byte_dir, s3_loc)
