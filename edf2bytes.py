#!/usr/bin/env python
# run "ln -s <this-script's-location> /usr/local/bin/edf2bytes" to make this script executable from anywhere
import argparse
import urlparse
import os
import sys
from boto import connect_s3, 

def local_and_s3_writer(edf, header, local, s3):
    local_writer(edf, header, local, s3)
    
    parsed_s3 = 
    conn = connect_s3()

    return

def local_writer(edf, header, local, s3):
    # create directory for byte files
    store_path = os.path.join(local,header['filename'],'')
    if not os.path.exists(store_path):
        os.makedirs(store_path)

    edf.seek(header['length']) # find beginning of data
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
    maxprogress = header['numRecs']*header['numSigs']
    # write data from edf to the file
    for i in range(header['numRecs']): # iterate over records
        record = edf.read(sum(header['numSamps'])*2) # read an entire record
        for j in range(header['numSigs']): # iterate over signals within records
            # grab and write signal data from record
            files[j].write(record[sigBounds[j][0]:sigBounds[j][1]])

            # progress bar
            currprogress = i*header['numRecs']+j
            relprogress = int((float(currprogress)/maxprogress)*50)
            sys.stdout.write("\r[" + "=" * relprogress +  " " * (50 - relprogress) + "]" +  str(relprogress*2) + "%")
    
    for f in files:
        f.close() # close files

    print "\nLocal write complete!"
    return

def head_parser(thisFile):
    header = {}
    header['filename'] = thisFile.name
    # extract info from header
    thisFile.read(184)
    header['length'] = int(thisFile.read(8).strip())
    thisFile.read(44)
    header['numRecs'] = int(thisFile.read(8).strip())
    thisFile.read(8)
    header['numSigs'] = int(thisFile.read(4).strip())
    header['sigLabels'] = []
    for i in range(header['numSigs']):
        header['sigLabels'].append(thisFile.read(16).strip())
    thisFile.read(header['numSigs']*(80+8+8+8+8+8+80))
    header['numSamps'] = []
    for i in range(header['numSigs']):
        header['numSamps'].append(int(thisFile.read(8).strip()))

if __name__ == '__main__':
    # set up and parse options
    parser = argparse.ArgumentParser()
    parser.add_argument('edfloc', help='Location of edf file to convert')
    parser.add_argument('local', help='Location to store binary files (One for each signal) on the local machine.')
    parser.add_argument('--s3', help='URI formatted location to store binary on S3.  Only works if you have AWS ' +
                                    'credentials stored as environment variables.')
    args = parser.parse_args()

    edf_file = args.edfloc
    byte_dir = args.local
    s3_loc = args.s3

    # set up file writer
    if not s3_loc and not byte_dir:
        sys.exit('Must provide an output location (either local (--local), S3 (--s3), or both).')
    elif s3_loc and byte_dir:
        writer = local_and_s3_writer
    else: # only byte_directory provided
        writer = local_writer

    
    # reads an edf file and splits the signals into a folder of binary files (one for each signal)
    with open(edf_file, 'r+b') as thisFile: # open edf file as read-binary
        # parse header
        header = head_parser(thisFile)

        # write to binary files
        writer(thisFile, header, byte_dir, s3_loc)





