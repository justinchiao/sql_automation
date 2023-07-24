### File and Paragraph naming convention
-Please use underscores(YCP_new_users) for sql file names and Zeppelin paragraph titles\
-Filenames with follow this notation: paragraphTitle_timeDateCompleted.csv\
-Folders will be named date and time completed\

### Change Date parameters in SQL
Example below will return data for the last week not including current day
```
and a_receive_day >= DATE_SUB(CURRENT_DATE(),7)
and f_timestamp_day >= DATE_SUB(CURRENT_DATE(),7) and f_timestamp_day <= DATE_SUB(CURRENT_DATE(),1)
```