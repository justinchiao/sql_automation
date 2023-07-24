import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
import os
import ctypes

def getQueries(noteID,user,password):
    '''Gets all text from each paragraph'''
    noteInfoQ = 'server' + noteID
    notebookInfo = requests.get(noteInfoQ, auth=HTTPBasicAuth(user, password)).json()['body']
    listParagraphs = notebookInfo['paragraphs']

    myPs = []
    for i in range(len(listParagraphs)):
        id = listParagraphs[i]['id']
        text = listParagraphs[i]['text']
        query = text.split('\n')
        myPs.append([id, query])
    # myPs is an array of paragraph id's and list of lines in that paragraph

def cleanResults(noteID,user,password):
    '''returns list of lists containing paragraph ID, Title, and Output [{id, title, timeFin, columns, data}, {id, title, timeFin, columns, data}, etc]'''

    #gets json with information about all paragraphs
    noteInfoQ = 'server' + noteID
    results = requests.get(noteInfoQ, auth=HTTPBasicAuth(user, password)).json()['body']['paragraphs']


    clean = []
    for i in range(len(results)):
        #first paragraph is to change output line limit settings and will not return output so it is skipped
        if i == 0:
            continue

        #extracts necessary information from nested dictionaries
        title = results[i]['title']
        id = results[i]['id']
        timeFinish = results[i]['dateFinished'].replace(' ','_').replace(',','').replace(':','.')
        columns = list(results[i]['config']['results']['0']['graph']['setting']['table']['tableColumnTypeState']['names'].keys())
        rawData = results[i]['results']['msg'][0]['data']
        
        #splits data from one long string into 1 string per row
        unsplitRows = rawData.split('\n')
        #splits each data row into list of strings
        listOfRows = []
        for i in range(len(unsplitRows)):
            if unsplitRows[i] == '':
                del unsplitRows[i]
            else:
                listOfRows.append(unsplitRows[i].split('\t'))

        #clean is the list of dictionaries to return
        clean.append({'id':id, 'title':title, 'timeFin':timeFinish, 'columns':columns , 'data':listOfRows[1:]})
    return clean

def getResults(noteID,user,password):
    '''Asynchronously runs all paragraphs and returns pharagraph results. Will check for completion at set interval'''

    #run all paragraphs, longer runtime notebooks will return 504, but notebook will still run
    runNote = 'server' + noteID
    requests.post(runNote, auth=HTTPBasicAuth(user, password))

    while True:
        #wait 1min
        time.sleep(60)

        #get paragraph status
        getStatus = 'server' + noteID
        allStatus = requests.get(getStatus, auth=HTTPBasicAuth(user, password)).json()['body']

        #check paragraph status
        for i in range(len(allStatus)):
            if allStatus[i]['status'] != 'FINISHED':
                break

            #if all are finished
            return cleanResults(noteID,user,password)

def export(array, folderName):
    '''takes list of dictionaries and exports each as its own csv'''

    for i in range(len(array)):
        dictionary = {}
        #gets column headers to use as dictionary keys
        keys = array[i]['columns']

        #restructures data from list per row to list per column
        for j in range(len(keys)):
            info=[]
            for k in range(len(array[i]['data'])):
                info.append(array[i]['data'][k][j])
            dictionary[keys[j]] = info
        
        #converts dictionary to pandas dataframe
        df = pd.DataFrame(dictionary)
        
        #saves dataframe as csv. file name is paragraph title + server finish date and time. server time is taiwan - 15hr (pacific standard time)
        path = folderName + '\\' + array[i]['title'] + '_' +array[i]['timeFin'] + '.csv'
        df.to_csv(path, encoding='utf-8',header=True)

    

def main():
    #change credentials
    user = '#'
    password = '#'
    noteID = '#'

    #clear old outputs
    clearOutput = 'server' + noteID + '/clear'
    requests.put(clearOutput, auth=HTTPBasicAuth(user, password))

    #restart interpreter to allow higher setting for output limit
    requests.put('server', auth=HTTPBasicAuth(user, password))
    
    #run all paragraphs
    array = getResults(noteID,user,password)

    #create folder with name current date and time
    now = datetime.now()
    folderName = now.strftime("%b_%d_%Y_%H.%M.%S")
    os.mkdir(folderName)

    #save data as csv
    export(array, folderName)

    #popup window upon completion
    ctypes.windll.user32.MessageBoxW(0, "Queries Complete", "", 0x00001000)

if __name__ == "__main__":
    main()