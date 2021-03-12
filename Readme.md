### Download DHS Files and Join Modules

#### Process

-   run [main.py](http://main.py)
-   check output\_info folder, update the ***hhs\_dhs\_join.csv***
    -   manully match the ***year*** column with ***SurveyYearLabel***
        columns on conditions where the year seem the same. For example,
        where HHS data submission says year 2015-16, but DHS year label
        is 2016.
    -   save the updated files as ***hhs\_dhs\_join\_updated.csv***
    -   an example is given in the
        ***output\_info//hhs\_dhs\_join\_updated\_example.csv***
-   run [main.py](http://main.py) again

#### Explanation of data files

##### Survey file formats

-   download all needed files in the stata format into stata\_files
    folder
-   some stata files couldn’t be read by pyreadstat (for now, two
    surveys have this problem)
-   these survey data files will be downloaded again but in sas format,
    in the sas\_files folder
-   then the module join for these surveys will be performed using the
    read in sas files
-   If survey data files fail to read in either sas or stata format,
    these surveys will be printed out
-   The process are automated, user just need to follow the above
    **Process section**

###### Other Data Folder

-   **source\_data//urlslist\_139855** are the download url generated
    from DHS website
-   ***result\_csv*** folder contain
    -   ***meta.csv*** metadata for all surveys
    -   ***response\_meta.csv*** response metadata for **stata**
        surveys*(Note: unable to read sas file response metadata)*
    -   literacy join results for each survey

