import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
from openpyxl import Workbook
from string import ascii_uppercase as auc
import os
  
# Path
path = 'Z:\CPI_Management\Monetization\Auto Project\weekly_output'
start = 'Users\Justin_Chiao\Documents\GitHub\sql-automation'

relative_path = os.path.relpath(path, start)
  
# Print the relative file path
# to the given path from the 
# the given start directory.
print(relative_path)

# wb = Workbook()
# wb.create_sheet("Mysheet")
# wb.save('balances.xlsx')
