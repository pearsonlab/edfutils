#!/usr/bin/env python
import argparse
import numpy as np
import h5py as h5
import os.path, os.makedirs

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('edfloc', help='Location of edf file to convert')
    parser.add_argument('h5loc', help='Location to store hdf5 files (One for each signal) on the local machine.')
    parse.add_argument('--s3', help='URI formatted location to store on S3.  Only works if you have AWS' +
                                    'credentials stored as environment variables.')
    args = parser.parse_args()
    edf_file = args.edfloc
    hdf5_dir = args.h5loc
    s3_loc = args.s3

