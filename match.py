# anti-join surveys in hhs data_submission with dhs available surveys
import requests
import pandas as pd
import numpy as np
import re
import json


def get_survey_api(url_path):
    # urls downloaded from DHS data access
    df_urls=pd.read_csv(url_path, sep="\n", header=None)
    df_urls.columns = ["url"]
    
    
    # extract survey number and filename information from the download url using regular expression
    df_urls=(df_urls.assign(SurveyNum=lambda x : x['url'].apply(lambda i: re.findall('surv_id=\d+',i)[0][8:]))
                    .assign(FileName=lambda x : x['url'].apply(lambda i: re.findall('Filename=\w+\Wzip',i)[0][9:].upper()))    
            )
    df_urls['SurveyNum']=df_urls['SurveyNum'].astype('int64')
    
    
    # Get all survey file info via DHS API
    r=requests.get('https://api.dhsprogram.com/rest/dhs/datasets?fileFormat=DT')
    result=json.loads(r.text)
    
    # write survey info into dataframe df_dhs_survey, change all file name to upper case
    df_dhs_survey=(pd.json_normalize(result['Data'])
                     .assign(FileName = lambda x :x['FileName'].apply(lambda i: i.upper()))
                  )
    
    
    # Get dhs country coding via API, df_country_code
    c=requests.get('https://api.dhsprogram.com/rest/dhs/countries')
    df_country_code=pd.json_normalize(json.loads(c.text)['Data'])
    
    
    
    # merge the url info with the survey info in df_dhs_survey
    df_dhs_survey=pd.merge(df_dhs_survey,df_urls,how='inner',on=['SurveyNum','FileName'])
    
    # merge with df_country_code to add ISO country code to the df
    # select only useful columns to keep
    df_dhs_survey=(pd.merge(df_dhs_survey,df_country_code,how='inner',on=['DHS_CountryCode','CountryName'])
                    .loc[:,['SurveyId','FileType','SurveyYearLabel','ISO3_CountryCode','DHS_CountryCode','url','FileName','CountryName']])
    
    
    
    # all the surveys on DHS with country and year info combination
    dhs_country_year=df_dhs_survey.loc[:,['SurveyYearLabel','DHS_CountryCode','CountryName','ISO3_CountryCode']].drop_duplicates()
    return [df_dhs_survey,dhs_country_year]

# function to get DHSs info from the HHS data Note column from the hhs dataset
def match_DHS(x):
    result=re.findall('^\w+\s+DHS\s\d+\W*\d+\W',x)
    if len(result)>0:
        return result[0][:-1].split(' ')
    else:
        return np.nan


def hhs_process(hhs_path,country_path):
    # Get year, country info from hhs data Note column
    df_hhs=(pd.read_csv(hhs_path)
              .assign(dhs=lambda x : x['NOTE'].apply(lambda i: match_DHS(i)))
    #         filter out those with DHS as source
              .query('dhs==dhs')
              .assign(year = lambda x : x['dhs'].apply(lambda i: i[2]))
              .assign(year = lambda x:x['year'].astype('str'))
              .assign(country =lambda x : x['dhs'].apply(lambda i: i[0]))
           )
    
    # UIS country code
    uis_country_code=pd.read_csv(country_path)
    
    
    # add country code to df_hhs
    # there are two entries of country name not in EDUN Country, manually fix these
    df_hhs.loc[df_hhs['country']=='Bolivia','country']='Bolivia (Plurinational State of)'
    df_hhs.loc[df_hhs['country']=='Swaziland','country']='Eswatini'
    
    df_hhs=pd.merge(uis_country_code,df_hhs,how='right',left_on='COUNTRY_NAME_EN',right_on='country')
    df_hhs.rename(columns={'COUNTRY_ID':'ISO'},inplace=True)
    
    
    # all the country year combination info in data submission which are from DHS
    hhs_country_year=df_hhs.loc[:,['year','country','ISO']].drop_duplicates()
    hhs_country_year.reset_index(inplace=True,drop=True)
    return hhs_country_year


# match the country year combination of DHS surveys with the survey info in HHS data submission
# write into csv and manually match the year which are supposed to match but not matching
def hhs_dhs_match():
    hhs_country_year=hhs_process('source_data//hhs.csv','source_data//EDUN_COUNTRY.csv')
    [df_dhs_survey,dhs_country_year]=get_survey_api('source_data//urlslist_139855.txt')
    year_country_match=pd.merge(hhs_country_year,dhs_country_year,how='outer',left_on=['year','ISO'],right_on=['SurveyYearLabel','ISO3_CountryCode'])
    year_country_match.to_csv('output_info//hhs_dhs_join.csv')
    df_dhs_survey.to_csv('output_info//survey_info.csv')


def construct_url_df():
    
    df_dhs_survey=(pd.read_csv('output_info//survey_info.csv')
                     .drop(columns='Unnamed: 0')
                   )
    
    # the read in file is an edited version of hhs_dhs_join after manual match
    # df_match_result is the anti join result, i.e. surveys not in hhs data submission
    
    df_match_result=(pd.read_csv('output_info//hhs_dhs_join_updated.csv')
                    #   filter which survey is not in hhs data submission, i.e. year is nan
                   .query('year!=year')
                   .loc[:,['SurveyYearLabel','ISO3_CountryCode']]
                )
    df_match_result.reset_index(inplace=True, drop=True)
    
    
    # join the anti-join result with the download url and survey info
    df_download=(pd.merge(df_match_result,df_dhs_survey,how='inner',on=['SurveyYearLabel','ISO3_CountryCode'])
                   .query('ISO3_CountryCode==ISO3_CountryCode')
                   .assign(FolderName = lambda x: x['ISO3_CountryCode']+'_'+x['SurveyYearLabel'])
                 )
    
    # write survey folder name and module folder name into the dataframe
    # Use country+year as folder name for survey
    # Add suffix to indicate module and as module folder name
    
    df_download.reset_index(inplace=True,drop=True)
    
    hh_filter=(df_download['FileType']=='Household Member Recode')
    women_filter=(df_download['FileType']=='Individual Recode')
    men_filter=(df_download['FileType']=="Men's Recode")
    
    
    df_download.loc[hh_filter,'SubFolderName']=df_download.loc[hh_filter,'FolderName']+'_hhm'
    df_download.loc[women_filter,'SubFolderName']=df_download.loc[women_filter,'FolderName']+'_wo'
    df_download.loc[men_filter,'SubFolderName']=df_download.loc[men_filter,'FolderName']+'_men'
    
    # We are going to use this csv to proceed to download
    return df_download
