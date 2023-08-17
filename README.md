### Required Packages
pip install python-dateutil\
pip install tableauhyperapi\
pip install pandas\
pip install requests\
pip install urllib3

### Running Zeppelin Notebook and Adding Data to Tableau
full_integration.py
1. Credentials, source, and destination\
Information can be set with variables or by asking user each time. To set once, comment out variable declaration that uses askInfo() and un-comment individual variable declarations. To ask user everytime, comment out individual variable declarations and un-comment variable declaration that uses askInfo(). When asking user, the window can have prefilled data. Set this by filling the string in ```version.insert(0, "") ``` for each field in askInfo. Default is set once and empty prefill.
2. CSV output\
Option is availible to have Zeppelin results downloaded as csv files. Files will appear in the same directory as full_integration.py. This option can be set in main() near line 655. Default is no csv output.
3. Time and Date warnings\
Server time is Taiwan minus 15 hours. Queries in zeppelin should be finished by 3PM Taiwan to avoid query of wrong days

### Backup files and overwriting errors
1. Before full_integration.py publishes a change, it will download the current datasource as a .tdsx file with datetime into a directory with the same name as the datasource. This file can be treated the same as .hyper files.
2. To revert to an older version, copy the .tdsx into the main directory and remove the datetime information so that the filename matches the datasource name. Use connect_with_tableau.py to overwrite the current datasource with the backup datasource. 

### Upload queries to Zeppelin
1. Zeppelin web interface\
Check that no blank paragraph is added at the bottom of the note. If one is added, remove it. 
2. upload_to_zeppelin.py\
Add queries as .txt to queries folder. Change credentials and destination in main(). Do not include %sql in your text. Running upload.py will add all queries to Zeppelin notebook specified by variable noteID. Use file and paragraph naming conventions described below. 

### Convert CSV to .hyper
convert_csv_.py
1. Change csv file in line 63
2. Change string in line 66 to the first column name. Due to unknown encoding issues the first string does not decode properly.
3. Change hyper filename in line 69. 
4. Hyper table name will be the hyper filename minus .hyper extension.
5. Check that the date format in isDate() matches the date format found in the csv.

### Adding .hyper as datasource to Tableau
connect_with_tableau.py
1. Credentials, filename, and destination project can be changed in main()
2. Append and overwrite setting can be changed in publish_datasource(). The last line of the blocks in both if/else will be something like this: ```publish_url += "&datasourceType={0}&overwrite=false&append=true".format(file_extension)```. Change append and overwrite settings by setting it to true or false. 
3. To append, there must be a datasource with the same name as the .hyper file in the specified project and overwrite must be false.
4. To create a new datasource, append must be false. 
5. If a data source exists in the project, append is false, and overwrite is false, error will be raised.
6. Both are default to false if not specified in request
7. Will download backup files to the same folders as full_integration.py before publishing changes.
8. Read more from Tableau: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#publish_data_source


### Known Bugs
1. Sometimes Zeppelin will return error - no interpreter set for this notebook. To resolve, save interpreter settings and reload the notebook page. This will result in full_integration.py and auto_query.py stuck trying to run all paragraphs.
2. If any Zeppelin paragraph returns an error, full_integration.py will retry running the whole notebook. There is no limit to number of retries.

### Change Date parameters in SQL for periodized queries
Example below will return data for the last week not including current day
```
and a_receive_day >= DATE_SUB(CURRENT_DATE(),7)
and f_timestamp_day >= DATE_SUB(CURRENT_DATE(),7) and f_timestamp_day <= DATE_SUB(CURRENT_DATE(),1)
```