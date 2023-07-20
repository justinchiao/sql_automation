import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
import os


def main():
    directory = 'queries'
    noteID = input("Enter notebook ID:")
 
    queriesList=[]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(f)
            query = open('queries\\' + filename)
            #print(query.read())
            queriesList.append({'title':filename, 'text':query.read()})
            query.close()
    print(queriesList)

    addP = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/' + noteID + '/paragraph'
    for i in range(len(queriesList)):
        jsonobj = json.dumps(queriesList[i])
        requests.post(addP, jsonobj, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX'))

if __name__ == "__main__":
    main()