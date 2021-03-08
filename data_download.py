# -*- coding: utf-8 -*-
# download survey data stata or sas from dhs website

import os 
import requests
import zipfile
# make a new directory if it doesn't exist
def makedirs(path):
    if os.path.exists(path)==False:
        os.makedirs(path)
        
# download data with url from dhs
def download(df,Num,majorfolder):
    headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36"}
    user_key = {
# input your userid and password
        'UserName': '',
        'UserPass': '',
        'Submitted':1,
        'UserType': 2,
        'submit': 'Sign In'
    }
# input your project_id
    project_info={'proj_id': '139855','action': 'getdatasets'}

    with requests.Session() as s:
        # Fill in login details to be posted to the login form.
        p = s.post('https://dhsprogram.com/data/dataset_admin/login_main.cfm', data=user_key)
        # Fill in projects details here to be posted to the project dropdown
        p2 = s.post('https://dhsprogram.com/data/dataset_admin/index.cfm', data=project_info)
        
        for i in range(0,Num):
            url = df.loc[i,'url']
            makedirs(majorfolder)
            
    #    Survey folder
            #   dir_survey is folder for the entire survey
            dir_survey = majorfolder+'//'+ df.loc[i,'FolderName']
            makedirs(dir_survey)

    #    Children folders

            #  create folder to store original download inside the survey directory
            dir_original_zip=dir_survey+'//original_downloaded_zipfiles'
            makedirs(dir_original_zip)
            #  dir_module is folder for the module
            dir_module = dir_survey+'//'+ df.loc[i,'SubFolderName']
            makedirs(dir_module)
            
        
    #    Grandchildren folders and files
    
            #  zip_name is the zip downloaded inside the data_download folder
            zip_name =  df.loc[i,'SubFolderName']+'.zip'
            zip_path =  dir_original_zip+'//'+ zip_name
            #  micro_data is stata data folder
            dir_micro_data = dir_module +'//microdata'
            makedirs(dir_micro_data)
            #  doc_data is documentation folder
            dir_doc_data = dir_module +'//documentation'
            makedirs(dir_doc_data)
           
            
    #  Check if zip file for the survey already downloaded, if not download and save
            #   get all downloaded zips in data_downloaded
            downloaded_zips=[]
            for root, dirs, files in os.walk(majorfolder, topdown=False):
                downloaded_zips.extend([name for name in files if name.lower().endswith('zip') ])
                
            #   if zip not downloaded, save zip from url  
            if zip_name not in downloaded_zips:
                r = s.get(url, allow_redirects=True)
                open(zip_path, 'wb').write(r.content)

            #   Extract zip file into the survey module folder  
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(dir_module)

            #   move documentation file into the documentation folder, other files in the microdata folder
            for root, dirs, files in os.walk(dir_module, topdown=False):
                    for name in files:
                        if name.lower().endswith('doc')|name.lower().endswith('pdf'):
                            os.replace(os.path.join(root, name),os.path.join(dir_doc_data, name))
                        else:
                            os.replace(os.path.join(root, name),os.path.join(dir_micro_data, name))
