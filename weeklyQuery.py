import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import pandas as pd
import os
import ctypes
# test notbook id: 2J4GY9RVJ

def getQueries(noteID):
    noteInfoQ = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/' + noteID
    notebookInfo = requests.get(noteInfoQ, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX')).json()['body']
    listParagraphs = notebookInfo['paragraphs']

    myPs = []
    for i in range(len(listParagraphs)):
        id = listParagraphs[i]['id']
        text = listParagraphs[i]['text']
        query = text.split('\n')
        myPs.append([id, query])
    # myPs is an array of paragraph id's and list of lines in that paragraph

def cleanResults(noteID):
    '''returns list of lists containing paragraph ID, Title, and Output [{id, title, timeFin, columns, data}, {id, title, timeFin, columns, data}, etc]'''
    #gets json with information about all paragraphs
    noteInfoQ = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/' + noteID
    results = requests.get(noteInfoQ, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX')).json()['body']['paragraphs']
    clean = []
    for i in range(len(results)):
        title = results[i]['title']
        id = results[i]['id']
        timeFinish = results[i]['dateFinished'].replace(' ','_').replace(',','').replace(':','.')
        columns = list(results[i]['config']['results']['0']['graph']['setting']['table']['tableColumnTypeState']['names'].keys())
        rawData = results[i]['results']['msg'][0]['data']
        unsplitRows = rawData.split('\n')
        listOfRows = []
        for i in range(len(unsplitRows)):
            if unsplitRows[i] == '':
                del unsplitRows[i]
            else:
                listOfRows.append(unsplitRows[i].split('\t'))
        clean.append({'id':id, 'title':title, 'timeFin':timeFinish, 'columns':columns , 'data':listOfRows})

    return clean

def getResults(noteID):
    #run all paragraphs, longer runtime notebooks will return 504, but notebook will still run
    runNote = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/job/' + noteID
    requests.post(runNote, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX'))

    while True:

        #wait 5min
        time.sleep(300)

        #get paragraph status
        getStatus = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/job/' + noteID
        allStatus = requests.get(getStatus, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX')).json()['body']

        #check paragraph status
        for i in range(len(allStatus)):
            if allStatus[i]['status'] != 'FINISHED':
                break

            #if all are finished
            return cleanResults(noteID)

def export(array, folderName):
    for i in range(len(array)):
        dictionary = {}
        keys = array[i]['columns']
        for j in range(len(keys)):
            info=[]
            for k in range(len(array[i]['data'])):
                info.append(array[i]['data'][k][j])
            dictionary[keys[j]] = info
        df = pd.DataFrame(dictionary)
        
        path = folderName + '\\' + array[i]['title'] + '_' +array[i]['timeFin'] + '.csv'
        df.to_csv(path, encoding='utf-8')

    

def main():
    #ask for notebook id
    noteID = input("Enter notebook ID:")

    #clear old outputs
    clearOutput = 'https://query-ntu.perfectcorp.com/zeppelin/api/notebook/' + noteID + '/clear'
    requests.put(clearOutput, auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX'))

    #restart interpreter to allow higher setting for output limit
    requests.put('https://query-ntu.perfectcorp.com/zeppelin/api/interpreter/setting/restart/spark', auth=HTTPBasicAuth('sinfulheinz', 'Tj7g&tENQ/d-PFnX'))
    
    #run all paragraphs
    array = getResults(noteID)

    #save data as csv
    now = datetime.now()
    folderName = now.strftime("%d_%m_%Y_%H.%M.%S")
    os.mkdir(folderName)
    export(array, folderName)

    ctypes.windll.user32.MessageBoxW(0, "Queries Complete", "", 0x00001000)

if __name__ == "__main__":
    main()