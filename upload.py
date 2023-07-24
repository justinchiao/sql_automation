import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
import os

def main():
    directory = 'queries'
    noteID = '2J4GY9RVJ'
    
    queriesList=[]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(f)
            txtFile = open('queries\\' + filename)
            query = '%sql\n' + '--'+ filename[:-4] + '\n\n' + txtFile.read()
            queriesList.append({'title':filename[:-4], 'text':query})
            txtFile.close()
    print(queriesList)

    addP = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/' + noteID + '/paragraph'
    for i in range(len(queriesList)):
        jsonobj = json.dumps(queriesList[i])
        requests.post(addP, jsonobj, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX'))

if __name__ == "__main__":
    main()