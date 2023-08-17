import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
import os

def main():
    '''
    Uploads all queries in queries folder to specified note ID
    '''
    directory = 'PATH_TO_QUERIES'
    noteID = 'DESTINATION_NOTE_ID'
    user = 'ZEPPELIN_USERNAME'
    password = 'ZEPPELIN_PASSWORD'
    
    ### creates a list of strings where eachs tring is a query from a text file. Adds %sql and a comment with file name to beginning of the text
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

    ### builds request to add paragraphs
    addP = 'https://ZEPPELIN_SERVER/zeppelin/api/notebook/' + noteID + '/paragraph'

    ### adds each query in the list as a new paragraph
    for i in range(len(queriesList)):
        jsonobj = json.dumps(queriesList[i])
        requests.post(addP, jsonobj, auth=HTTPBasicAuth(user, password))

if __name__ == "__main__":
    main()