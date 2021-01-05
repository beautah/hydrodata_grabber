# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 13:24:33 2021

@author: buriona
"""

import sys
import time
from os import path, makedirs
import pandas as pd
from requests import get as r_get
from io import StringIO

BASE_URL = 'https://www.usbr.gov/uc/water/hydrodata'
WAIT = 3

def parse_url(site_id=None, datatype_id=None, meta=False, 
              obj_type='reservoir', base=BASE_URL):
    
    if meta:
        return f'{BASE_URL}/{obj_type}_data/meta.csv'
    return f'{BASE_URL}/{obj_type}_data/{site_id}/csv/{datatype_id}.csv'

def export_df(df, output_path, output_format='csv'):
    if output_format =='csv':
        df.to_csv(output_path, index=False)
    else:
        df.to_json(output_path, orient='split', index=False)

def get_meta(obj_type='reservoir', output_dir='', output_format='csv', updt=False):
     
    url = parse_url(meta=True, obj_type=obj_type)
    try:
        csv_str = r_get(url, timeout=10).text
        csv_io = StringIO(csv_str)
        df = pd.read_csv(csv_io)
    except Exception as err:
        return f'An error occured getting metadata from {url}, no data was retrieved, shutting down - {err}'
        sys.exit(1)
        
    if updt:
        meta_dir = path.join(output_dir, 'meta')
        makedirs(meta_dir, exist_ok=True)
        output_path = path.join(meta_dir, f'{obj_type}_meta.{output_format}')
        export_df(df, output_path, output_format=output_format)
        print(f'  Saved {obj_type} metadata {output_format} here: {output_path}')
    return df

def get_data(meta_row, obj_type='reservoir', output_dir='', 
             output_format='csv'):
    
    site_name = meta_row['site_metadata.site_common_name'].lower()
    datatype_name = meta_row['datatype_metadata.datatype_common_name'].lower()
    units = meta_row['datatype_metadata.unit_common_name'].lower()
    site_id = meta_row['site_id']
    datatype_id = meta_row['datatype_id']
    url = parse_url(site_id=site_id, datatype_id=datatype_id, obj_type=obj_type)
    try:
        csv_str = r_get(url, timeout=10).text
        csv_io = StringIO(csv_str)
        df = pd.read_csv(csv_io)
        df.rename(
            columns={
                'datetime': 'datetime (MST)', 
                datatype_name: f'{datatype_name} ({units})'
            }
        )
        output_path = path.join(output_dir, f'{site_name}_{datatype_name}.{output_format}')
        if df.empty:
            return 'No data was returned from {url}, no data was saved.'
        export_df(df, output_path, output_format=output_format)
    except Exception as err:
        return f'An error occured getting data from {url}, no data was saved - {err}'
    return f'Saved {datatype_name} for {site_name} as a {output_format}.'

def parse_args(parser):
    
    parser.add_argument(
        "-V", "--version", help="show program version", action="store_true"
    )
    parser.add_argument("-o", "--output", help="output dir", type=str, default='')
    parser.add_argument(
        "-f", "--format", help="output format", default='csv', 
        choices=['csv', 'json']
    )
    parser.add_argument(
        "-t", "--type", help="reservoir or gage data", default='reservoir', 
        choices=['reservoir', 'gage']
    )
    parser.add_argument(
        "-m", "--meta", 
        help="update metadata, will stop after, no timeseries data will be gathered", 
        action="store_true"
    )
    parser.add_argument(
        "-s", "--site", action='append', type=str,
        help="Update data for a given site_id, can provide more than one using this flag. Skip for all sites."
    )
    parser.add_argument(
        "-d", "--datatype", action='append', type=str,
        help="Update data for a given datatype_id, can provide more than one using this flag. Skip for all sites."
    )
    return parser.parse_args()

if __name__ == '__main__':
    
    import argparse
    cli_desc = f'Gather and save data from BOR hydroData suite: {BASE_URL}/nav.html'
    parser = argparse.ArgumentParser(description=cli_desc)
    args = parse_args(parser)
    
    if args.version:
        print('v0.1')
        sys.exit(0)
     
    output_dir = path.dirname(path.realpath(__file__))
    
    if args.output:
        if path.isdir(args.output):
            output_dir = args.output
        else:
            print(f'{args.output} is not a valid directory, please try again.')
            sys.exit(1)
            
    updt_meta = False
    if args.meta:
        updt_meta = True
    
    print(
        f'Gathering metadata for {args.type} hydroData...\n'
    )
    df_meta = get_meta(
        obj_type=args.type,
        output_dir=output_dir,
        output_format=args.format,
        updt=updt_meta
    ).drop_duplicates()
    
    df_meta['site_id'] = df_meta['site_id'].astype(str)
    df_meta['datatype_id'] = df_meta['datatype_id'].astype(str)
    if updt_meta:
        sys.exit(0)
    
    if args.site:
        df_meta = df_meta[df_meta['site_id'].isin(args.site)]
    if args.datatype:
        df_meta = df_meta[df_meta['datatype_id'].isin(args.datatype)]
    
    data_dir = path.join(output_dir, f'{args.type}_data')
    makedirs(data_dir, exist_ok=True)
    
    n_tot = len(df_meta.index)
    n = 1
    print(
        f'Gathering data for {n_tot} datatypes and saving here:\n'
        f'  {data_dir}'
    )
    for idx, meta_row in df_meta.iterrows():
        result = get_data(
            meta_row, 
            obj_type=args.type, 
            output_dir=data_dir, 
            output_format=args.format
        )
        print(
            f'    ({n}/{n_tot}) - {result}\n'
            f'      Waiting {WAIT} seconds to play nice ;)'
        )
        for t in range(0, WAIT):
            time.sleep(1)
        n += 1