# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 13:43:00 2021

@author: y_wang3
"""

import pandas as pd
import os
import re


from match import hhs_dhs_match as antijoin
from match import construct_url_df
from data_download import download
from literacy_join import combinemeta, walk_4_data
from literacy_join import li_vars as li_vars

        
# download and process stata files
def stata_download_process(url_path,stata_folder,failed_file_path):
    global li_vars
    df=(pd.read_csv(url_path)
      .drop(columns='Unnamed: 0')
    )
    # download the stata files with the url df
    download(df,df.shape[0],stata_folder)
    
    # FileName is datafile name
    # path is datafile path
    df_url=(df.assign(FileName = lambda x: x['FileName'].apply(lambda o: re.sub('DT.ZIP','FL.DTA',o)))
              .assign(path =lambda x: stata_folder+'//'+x['FolderName']+'//'+x['SubFolderName']+'//microdata//'+x['FileName'])
            )
    
    # process the stata files 
    [df_meta,failed_surveys]=walk_4_data(df_url,stata_folder,li_vars)
    # write the metadata info in csv
    combinemeta(df_meta)
    
    # if there are surveys failed to read, write their info into csv
    if (len(failed_surveys)>0):
        print('failed to read surveys'+ (' ').join(failed_surveys))
        df_url_failed=df_url[df_url['FolderName'].isin(failed_surveys)]
        df_url_failed.reset_index(inplace=True,drop=True)
        df_url_failed.to_csv(failed_file_path)
    print('stata download and processing complete')
    

#  if pyreadstat failed to read the stata files, 
#  try download sas files and process them to get literacy result 
def failed_sas_download_process(url_path,sas_folder):
    global li_vars
    files=os.listdir('output_info')
    
    # check if the last step output a failed url result
    if os.path.basename(url_path) in files:
        failed_urls=(pd.read_csv(url_path)
                       .drop(columns='Unnamed: 0'))
        
        # adjust the url, FileName and path to represent the sas files
        failed_urls=(failed_urls
                     .assign(FileName = lambda x: x['FileName'].apply(lambda o: re.sub('FL.DTA','FL.SAS7BDAT',o)))
                     .assign(url = lambda x: x['url'].apply(lambda o: re.sub('DT.zip','SD.zip',o)))
                     .assign(path =lambda x: sas_folder+'//'+x['FolderName']+'//'+x['SubFolderName']+'//microdata//'+x['FileName'])
                     )
        # download the sas files with the url df
        download(failed_urls,failed_urls.shape[0],sas_folder)
        
        # process the sas files 
        [df_meta,failed_surveys]=walk_4_data(failed_urls,sas_folder,li_vars)
        # append new meta data to the meta.csv
        combinemeta(df_meta)
        
        # check if there are still surveys we are unable to read and process
        # if so print them out
        if (len(failed_surveys)>0):
            print('failed to read surveys'+ (' ').join(failed_surveys))
        print('sas download and processing complete')
   
        
    
def main():      
    files=os.listdir('output_info')
    if 'hhs_dhs_join_updated.csv' in files:
        construct_url_df().to_csv('output_info//url_download.csv')
    else:
        antijoin()
        print('Please manually update dhs_hhs_join and save it as dhs_hhs_join_update in the output info folder')
        return
    stata_download_process('output_info//url_download.csv','stata_files','output_info//failed_stata.csv')
    failed_sas_download_process('output_info//failed_stata.csv','sas_files')
    
main()