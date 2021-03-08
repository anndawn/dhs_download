# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 13:53:05 2021

@author: y_wang3
"""

import os
import pandas as pd
import pyreadstat
import functools
from data_download import makedirs

li_vars=['v155','v156','mv155','mv156','v108','mv108','mcaseid','hhid','caseid','hv001','hv002','hvidx','hv005','mv001','mv002','mv003','mv005','v001','v002','v003','v005']
li_vars.extend([i.upper() for i in li_vars])


# given data file path, and url df
# return survey info
def get_survey_info(df_url,filepath):
    survey_obj={}
    filter0=(df_url['path']==filepath)
    survey_obj['survey']=df_url.loc[filter0,'SurveyId'].values[0]
    survey_obj['module']=df_url.loc[filter0,'FileType'].values[0]
    survey_obj['year']=df_url.loc[filter0,'SurveyYearLabel'].values[0]
    survey_obj['iso']=df_url.loc[filter0,'ISO3_CountryCode'].values[0]
    survey_obj['filepath']=df_url.loc[filter0,'path'].values[0]
    return survey_obj


# given url df, data file path and the relevant variables to read
# return data and metadata df
def get_data(df_url,filepath,variables):
    
    survey_obj=get_survey_info(df_url,filepath)
    # read stata or sas file
    if filepath.lower().endswith('.dta'):
        df,meta= pyreadstat.read_dta(filepath,apply_value_formats=True,usecols=variables)
    elif filepath.lower().endswith('.sas7bdat'):
        df,meta= pyreadstat.read_sas7bdat(filepath,usecols=variables)
        
    df['survey']=survey_obj['survey']        
    df.columns = df.columns.str.upper()
    
    # change the variable names for household number, cluster number and line number
    # so it matches with the household memeber module
    # HV001, HV002 and HVIDX are the variables we join different modules on
    if survey_obj['module']=='Individual Recode':
        df.rename(columns={'V001':'HV001','V002':'HV002','V003':'HVIDX'},inplace=True)
    elif survey_obj['module']=="Men's Recode":
        df.rename(columns={'MV001':'HV001','MV002':'HV002','MV003':'HVIDX'},inplace=True)
                    
    # store the survey infomation in the metadata file as well
    meta_dict = meta.column_names_to_labels
    df_meta = (pd.DataFrame(list(meta_dict.items()),columns = ['var','label']) 
                        .assign(module=survey_obj['module'])
                        .assign(survey=survey_obj['survey'])
                        .assign(ISO=survey_obj['iso'])
                        .assign(year=survey_obj['year'])
                      )
    return [df,df_meta]




# given url df, a folder for all surveys, and relevant variables
# get data for each survey, combine modules on selected variables
# write literacy results for each survey in csv
# return metadata for all surveys and surveys failed to read

def walk_4_data(df_url,datapath,variables):
    
    makedirs('result_csv')
    
    #   all_obj to store all the dataframes for all surveys
    #   df_meta to store all metadata infomation for all surveys
    #   failed to store the data files failed to read
    all_obj={}
    df_meta=pd.DataFrame(columns=['var','label','survey','module','ISO','year'])
    failed=[]
    
    #  a list of all the survey folders under data_downloaded
    list_survey_folders = [f.path for f in os.scandir(datapath)]
    #  a list of surveys processed already
    list_survey_processed= [f.name for f in os.scandir('result_csv')]
    

    for survey_folder in list_survey_folders: 
        survey=os.path.basename(survey_folder)
        
        # check if the survey is already processed
        if (survey+'.csv') not in list_survey_processed:
            #   for each survey create a list of dfs to store different modules
            #   files are all the files under survey_folder
            files=[]
            dfs=[]
            for (dirpath, dirnames, filenames) in os.walk(survey_folder):
                files.extend(filenames)
            
            for name in files:
                # find dta and sas files in all files
                if name.lower().endswith('.sas7bdat')|name.lower().endswith('.dta'):
                    #  locate the survey and module based on name of data file
                    filepath=df_url.loc[df_url['FileName']==name.upper(),'path'].values[0]
                    
                    try:
                        # read data from data file
                        [df_single,df_single_meta]=get_data(df_url,filepath,li_vars)
                        # append metadata into df_meta
                        df_meta=df_meta.append(df_single_meta)
                        # append the microdata for the module into dfs
                        dfs.append(df_single)
                    except:
                        #  if unable to read any module in a survey, add the survey to list failed
                        failed.append(survey)
              
            # if there is more than one module for the survey 
            # join the modules and output csv for literacy variables
            if  len(dfs)>1:
                df_final = functools.reduce(lambda left,right: pd.merge(left,right,on=['SURVEY','HV001','HV002','HVIDX'],how='outer'), dfs)
                df_final.to_csv('result_csv//'+survey+'.csv',encoding='utf-8-sig')

    return [df_meta,failed]


# write walk_4_data meta result into csv
def combinemeta(new_meta_df):
    # if there is already a meta.csv,
    # read in and append the new meta data into the csv
    if os.path.exists('result_csv//meta.csv')==True:
        meta_new=(pd.read_csv('result_csv//meta.csv')
                  .drop(columns='Unnamed: 0')
                  .append(new_meta_df))
        meta_new.drop_duplicates(inplace=True)
        meta_new.reset_index(inplace=True,drop=True)
        meta_new.to_csv('result_csv//meta.csv')
    # otherwise write the meta into csv directly
    else:
        new_meta_df.to_csv('result_csv//meta.csv')