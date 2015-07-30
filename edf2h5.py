#!/usr/bin/env python
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('edfloc', help='Location of edf file to convert')
    parser.add_argument('h5loc', help='Location to store hdf5 files (One for each signal).' +
                                        'Can specify an S3 location (in format "s3://bucket-name/folder-name")' +
                                        'if your AWS credentials are stored as environment variables')
    args = parser.parse_args()
