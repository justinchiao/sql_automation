import requests
from requests.auth import HTTPBasicAuth
import datetime as dt
import time
import pandas as pd
import os
import ctypes
import xml.etree.ElementTree as ET
import math
import os
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata
from tkinter import *
from sys import exit

def cleanResults(noteID,user,password):
    '''returns list of lists containing paragraph ID, Title, and Output [{id, title, timeFin, columns, data}, {id, title, timeFin, columns, data}, etc]'''
    print('Formatting Results')
    ### gets json with information about all paragraphs
    noteInfoQ = 'https://ZEPPELIN_SERVER/zeppelin/api/notebook/' + noteID
    results = requests.get(noteInfoQ, auth=HTTPBasicAuth(user, password)).json()['body']['paragraphs']

    clean = []
    for i in range(len(results)):
        ### first paragraph is to change output line limit settings and will not return output so it is skipped
        if i == 0:
            continue

        ### extracts necessary information from nested dictionaries
        title = results[i]['title']
        id = results[i]['id']
        timeFinish = results[i]['dateFinished'].replace(' ','_').replace(',','').replace(':','.')
        columns = list(results[i]['config']['results']['0']['graph']['setting']['table']['tableColumnTypeState']['names'].keys())
        rawData = results[i]['results']['msg'][0]['data']
        
        ### splits data from one long string into 1 string per row
        unsplitRows = rawData.split('\n')
        ### splits each data row into list of strings
        listOfRows = []
        for i in range(len(unsplitRows)):
            if unsplitRows[i] == '':
                del unsplitRows[i]
            else:
                listOfRows.append(unsplitRows[i].split('\t'))

        ### clean is the list of dictionaries to return
        clean.append({'id':id, 'title':title, 'timeFin':timeFinish, 'columns':columns , 'data':listOfRows[1:]})
    return clean

def getResults(noteID,user,password):
    '''Asynchronously runs all paragraphs and returns pharagraph results. Will check for completion at set interval'''

    ### run all paragraphs, longer runtime notebooks will return 504, but notebook will still run
    runNote = 'https://ZEPPELIN_SERVER/zeppelin/api/notebook/job/' + noteID
    requests.post(runNote, auth=HTTPBasicAuth(user, password))

    while True:
        ### wait time in seconds between status checks
        time.sleep(30)

        ### get paragraph status
        getStatus = 'https://ZEPPELIN_SERVER/zeppelin/api/notebook/job/' + noteID
        allStatus = requests.get(getStatus, auth=HTTPBasicAuth(user, password)).json()['body']
        
        ### creates list of status
        status = []
        for i in range(len(allStatus)):
            status.append(allStatus[i]['status'])
        print(status)

        ### check paragraph status
        if 'ERROR' in status: # if any paragraphs error, retry
            print('Zeppelin error: Try Again')
            main()
        elif 'RUUNNING' in status or 'READY' in status or 'PENDING' in status or 'ABORT' in status: # if any paragraphs are not finished, continue loop and chekc again after sleep time
            continue
        else:
            for i in range(len(status)):
                if status[i] == 'FINISHED'and i != len(status)-1:
                    continue
                elif status[i] == 'FINISHED' and i == len(status)-1:
                    return cleanResults(noteID,user,password)
                else:
                    break
            continue

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

# The namespace for the REST API is 'http://tableau.com/api' for Tableau Server 9.1 or later
xmlns = {'t': 'http://tableau.com/api'}
# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB
# For when a data source is over 64MB, break it into 5MB (standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5    # 5MB

class ApiCallError(Exception):
    pass

class UserDefinedFieldError(Exception):
    pass

class WindowsInhibitor:
    '''Prevent OS sleep/hibernate in windows; code from:
    https://github.com/h3llrais3r/Deluge-PreventSuspendPlus/blob/master/preventsuspendplus/core.py
    API documentation:
    https://msdn.microsoft.com/en-us/library/windows/desktop/aa373208(v=vs.85).aspx'''
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001

    def __init__(self):
        pass

    def inhibit(self):
        import ctypes
        print("Preventing Windows from going to sleep")
        ctypes.windll.kernel32.SetThreadExecutionState(
            WindowsInhibitor.ES_CONTINUOUS | \
            WindowsInhibitor.ES_SYSTEM_REQUIRED)

    def uninhibit(self):
        import ctypes
        print("Allowing Windows to go to sleep")
        ctypes.windll.kernel32.SetThreadExecutionState(
            WindowsInhibitor.ES_CONTINUOUS)

def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.

    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')

def _make_multipart(parts):
    """
    Creates one "chunk" for a multi-part upload

    'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).

    Returns the post body and the content type string.

    For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
    """
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type

def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.

    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return

def sign_in(server, username, password, site=""):
    """
    Signs in to the server specified with the given credentials

    'server'   specified server address
    'username' is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format('3.2')

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ET.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    user_id = parsed_response.find('.//t:user', namespaces=xmlns).get('id')
    return token, site_id, user_id

def sign_out(server, auth_token, version):
    """
    Destroys the active session and invalidates authentication token.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(version)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 204)
    return

def start_upload_session(server, auth_token, site_id, version):
    """
    Creates a POST request that initiates a file upload session.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    Returns a session ID that is used by subsequent functions to identify the upload session.
    """
    print(auth_token)
    url = server + "/api/{0}/sites/{1}/fileUploads".format(version, site_id)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 201)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    return xml_response.find('t:fileUpload', namespaces=xmlns).get('uploadSessionId')

def get_default_project_id(server, auth_token, site_id, version, projectName):
    """
    Returns the project ID for the 'default' project on the Tableau server.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    """
    ### name of destination project
    page_num, page_size = 1, 100  # Default paginating values

    ### builds the request
    url = server + "/api/{0}/sites/{1}/projects".format(version, site_id)
    paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)
    server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))

    ### used to determine if more requests are required to find all projects on server
    total_projects = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
    max_page = int(math.ceil(total_projects / page_size))

    projects = xml_response.findall('.//t:project', namespaces=xmlns)

    ### continue querying if more projects exist on the server
    for page in range(2, max_page + 1):
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page)
        server_response = requests.get(paged_url, headers={'x-tableau-auth': auth_token})
        _check_status(server_response, 200)
        xml_response = ET.fromstring(_encode_for_display(server_response.text))
        projects.extend(xml_response.findall('.//t:project', namespaces=xmlns))

    ### look through all projects (EN and DE locales)
    for project in projects:
        if project.get('name') == projectName:
            return project.get('id')
    print("\tProject was not found in {0}".format(server))

def existing(server, auth_token, site_id, datasource_name, version):
    '''
    Checks through ALL data sources for given datasource name. In addition to determining if create datasource or append datasource is appropriate. This has the side effect of preventing creation of datasources with the same name in tableau site, by allowing the program to attempt append to a datasource that doesn't exist in the desired project. This will return an error. 
    '''

    ### get all datasources on current site
    url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    datasources = xml_response.findall('.//t:datasource', namespaces=xmlns)
    
    ### check if the name of the datasource we want to add already exists in the current list if datasource names on the site
    for datasource in datasources:
        if datasource_name == datasource.get('name'):
            print('existing')
            return True
    print('new')    
    return False

def publish_new_datasource(server, auth_token, site_id, datasource_filename, dest_project_id, version):
    """
    Publishes the data source to the desired project.

    'server'               specified server address
    'auth_token'           authentication token that grants user access to API calls
    'site_id'              ID of the site that the user is signed into
    'datasource_filename'  filename of data source to publish
    'dest_project_id'      ID of peoject to publish to
    """
    datasource_name, file_extension = datasource_filename.split('.', 1)
    datasource_size = os.path.getsize(datasource_filename)
    chunked = datasource_size >= FILESIZE_LIMIT

    ### build a general request for publishing
    xml_request = ET.Element('tsRequest')
    datasource_element = ET.SubElement(xml_request, 'datasource', name=datasource_name)
    ET.SubElement(datasource_element, 'project', id=dest_project_id)
    xml_request = ET.tostring(xml_request)

    if chunked:
        print("\tPublishing '{0}' in {1}MB chunks (data source over 64MB):".format(datasource_name, CHUNK_SIZE / 1024000))
        ### initiates an upload session
        print(auth_token)
        upload_id = start_upload_session(server, auth_token, site_id, version)

        ### URL for PUT request to append chunks for publishing
        put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(version, site_id, upload_id)

        ### reads and uploads chunks of the data source
        with open(datasource_filename, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),
                                                         'tableau_file': ('file', data, 'application/octet-stream')})
                print("\tPublishing a chunk...")
                server_response = requests.put(put_url, data=payload,
                                               headers={'x-tableau-auth': auth_token, "content-type": content_type})
                _check_status(server_response, 200)

        ### finish building request for chunking method
        payload, content_type = _make_multipart({'request_payload': ('', xml_request, 'text/xml')})

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
        publish_url += "?uploadSessionId={0}".format(upload_id)
        publish_url += "&datasourceType={0}&overwrite=false".format(file_extension)
    else:
        print("\tPublishing '{0}' using the all-in-one method (data source under 64MB)".format(datasource_name))

        ### read the contents of the file to publish
        with open(datasource_filename, 'rb') as f:
            datasource_bytes = f.read()

        ### finish building request for all-in-one method
        parts = {'request_payload': ('', xml_request, 'text/xml'),
                 'tableau_datasource': (datasource_filename, datasource_bytes, 'application/octet-stream')}
        payload, content_type = _make_multipart(parts)

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
        publish_url += "?datasourceType={0}&overwrite=false".format(file_extension)

    ### make the request to publish and check status code
    print("\tUploading...")
    server_response = requests.post(publish_url, data=payload,
                                    headers={'x-tableau-auth': auth_token, 'content-type': content_type})
    _check_status(server_response, 201)
    
def publish_datasource(server, auth_token, site_id, datasource_filename, dest_project_id, version, datasource_name):
    """
    Publishes the data source to the desired project.

    'server'               specified server address
    'auth_token'           authentication token that grants user access to API calls
    'site_id'              ID of the site that the user is signed into
    'datasource_filename'  filename of data source to publish
    'dest_project_id'      ID of peoject to publish to
    """
    ### Check if the datasource already exists in tableau. If no, a new datasource will be published
    if existing(server, auth_token, site_id, datasource_name, version) == False:
        publish_new_datasource(server, auth_token, site_id, datasource_filename, dest_project_id, version)
        return

    datasource_name, file_extension = datasource_filename.split('.', 1)
    datasource_size = os.path.getsize(datasource_filename)
    chunked = datasource_size >= FILESIZE_LIMIT
    ### build a general request for publishing
    xml_request = ET.Element('tsRequest')
    datasource_element = ET.SubElement(xml_request, 'datasource', name=datasource_name)
    ET.SubElement(datasource_element, 'project', id=dest_project_id)
    xml_request = ET.tostring(xml_request)

    if chunked:
        print("\tAppending '{0}' in {1}MB chunks (data source over 64MB):".format(datasource_name, CHUNK_SIZE / 1024000))
        ### initiates an upload session
        print(auth_token)
        upload_id = start_upload_session(server, auth_token, site_id, version)

        ### URL for PUT request to append chunks for publishing
        put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(version, site_id, upload_id)

        ### reads and uploads chunks of the data source
        with open(datasource_filename, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),
                                                         'tableau_file': ('file', data, 'application/octet-stream')})
                print("\tPublishing a chunk...")
                server_response = requests.put(put_url, data=payload,
                                               headers={'x-tableau-auth': auth_token, "content-type": content_type})
                _check_status(server_response, 200)

        ### finish building request for chunking method
        payload, content_type = _make_multipart({'request_payload': ('', xml_request, 'text/xml')})

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
        publish_url += "?uploadSessionId={0}".format(upload_id)
        publish_url += "&datasourceType={0}&append=true".format(file_extension)
    else:
        print("\tAppending '{0}' using the all-in-one method (data source under 64MB)".format(datasource_name))

        ### read the contents of the file to publish
        with open(datasource_filename, 'rb') as f:
            datasource_bytes = f.read()

        ### finish building request for all-in-one method
        parts = {'request_payload': ('', xml_request, 'text/xml'),
                 'tableau_datasource': (datasource_filename, datasource_bytes, 'application/octet-stream')}
        payload, content_type = _make_multipart(parts)

        publish_url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
        publish_url += "?datasourceType={0}&append=true".format(file_extension)

    ### make the request to publish and check status code
    print("\tUploading...")
    server_response = requests.post(publish_url, data=payload,
                                    headers={'x-tableau-auth': auth_token, 'content-type': content_type})
    _check_status(server_response, 201)

def delete_datasource(datasource_filename):
    os.remove(datasource_filename)

def isDate(string):
    ### try if the string is a date in the correct format as dictedt by the server
    try:
        t = dt.datetime.strptime(string, "%Y-%m-%d")
        return True, t
    except ValueError as err:
        return False, 'not a date'

def convertToHyper(dictionary, filename):
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp') as hyper:
        ### create the extract, replace it if it already exists
        
        ### iso alpha2 country codes
        alpha2 = ['AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BH', 'BS', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA', 'CV', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CG', 'CD', 'CK', 'CR', 'CI', 'HR', 'CU', 'CW', 'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE', 'RO', 'RU', 'RW', 'BL', 'SH', 'KN', 'LC', 'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SZ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU', 'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW']

        ### set column name and type and set data values to correct type
        columns = []
        for i in range(len(dictionary['columns'])):
            columnName = dictionary['columns'][i]
            sample = dictionary['data'][2][i]
            #print(sample)
            if sample.isnumeric(): #checks if value is an interger
                type = SqlType.int()
                for j in range(len(dictionary['data'])):
                    dictionary['data'][j][i] = int(dictionary['data'][j][i])
            elif sample.isdecimal(): #checks if value is an float
                type = SqlType.double()
                for j in range(len(dictionary['data'])):
                    dictionary['data'][j][i] = float(dictionary['data'][j][i])
            elif True in isDate(sample): #checks if value is a date in the same format as the database
                type = SqlType.date()
                for j in range(len(dictionary['data'])):
                    dictionary['data'][j][i] = isDate(dictionary['data'][j][i])[1]
            elif sample == 2: #checks if value is shorter than 2
                if sample in alpha2: #checks if value is in the list of alpha2 country codes
                    type = SqlType.geography()
                    for j in range(len(dictionary['data'])):
                        dictionary['data'][j][i] = bytes(dictionary['data'][j][i], 'utf-8')
            else: 
                type = SqlType.text()
                
            columns.append(TableDefinition.Column(columnName, type)) #add current column to list of columns

        with Connection(hyper.endpoint, filename, CreateMode.CREATE_AND_REPLACE) as connection: #create 
            schema = TableDefinition(dictionary['title'], columns)
            connection.catalog.create_table(schema)
            with Inserter(connection, schema) as inserter:
                inserter.add_rows(dictionary['data'])
                inserter.execute()

def appendTableau(dictionary, dest_server, dest_auth_token, dest_site_id, dest_project_id, version):
    '''
    Takes one item from array {id, title, timeFin, columns, data} and publishes to tableau
    '''
    ### initialization
    datasource_filename = dictionary['title'] + '.hyper'
    datasource_name = dictionary['title']

    ### convert data to .hyper
    #print('\nConverting data to .hyper format')
    convertToHyper(dictionary, datasource_filename)

    ### publish to new site
    #print("\nPublishing data source to {0}".format(dest_server))
    publish_datasource(dest_server, dest_auth_token, dest_site_id, datasource_filename, dest_project_id, version, datasource_name)

    ### Deleting data source from the source site #####
    #print("\nDeleting temp file")
    delete_datasource(datasource_filename)

def askInfo():
    root = Tk()
    # Field 1
    l1 = Label(text='Zeppelin Username:')
    l1.grid(row=0,column=0)
    zeppelinUser = Entry(root)
    zeppelinUser.insert(0, "") # fill string to prefill field
    zeppelinUser.grid(row=0,column=1)

    # Field 2
    l2 = Label(text='Zeppelin Password:')
    l2.grid(row=1,column=0)
    zeppelinPass = Entry(root)
    zeppelinPass.insert(0, "") # fill string to prefill field
    zeppelinPass.grid(row=1,column=1)

    # Field 3
    l3 = Label(text='Zeppelin Note ID:')
    l3.grid(row=2,column=0)
    notebook = Entry(root)
    notebook.insert(0, "") # fill string to prefill field
    notebook.grid(row=2,column=1)

    # Field 4
    l4 = Label(text='Tableau Username:')
    l4.grid(row=3,column=0)
    tableauUser = Entry(root)
    tableauUser.insert(0, "") # fill string to prefill field
    tableauUser.grid(row=3,column=1)

    # Field 5
    l5 = Label(text='Tableau Password:')
    l5.grid(row=4,column=0)
    tableauPass = Entry(root)
    tableauPass.insert(0, "") # fill string to prefill field
    tableauPass.grid(row=4,column=1)

    # Field 6
    l6 = Label(text='Tableau Project name:')
    l6.grid(row=5,column=0)
    tableauProject = Entry(root)
    tableauProject.insert(0, "") # fill string to prefill field
    tableauProject.grid(row=5,column=1)

    # Field 7
    l7 = Label(text='Tableau API Version:')
    l7.grid(row=6,column=0)
    version = Entry(root)
    version.insert(0, "") # fill string to prefill field
    version.grid(row=6,column=1)

    var = IntVar()
    b1=Button(root, text='submit', command = lambda: var.set(1))
    b1.grid(row=8,column=1)
    b1.wait_variable(var)
    
    zU = zeppelinUser.get()
    zP = zeppelinPass.get()
    noteID = notebook.get()
    tU = tableauUser.get()
    tP = tableauPass.get()
    tProj = tableauProject.get()
    ver = version.get()
    root.destroy()
    root.mainloop()

    return zU, zP, noteID,tU, tP, tProj, ver

def main():

    ### in Windows, prevent the OS from sleeping while we run
    osSleep = None
    if os.name == 'nt':
        osSleep = WindowsInhibitor()
        osSleep.inhibit()

    ### CHANGE CREDENTIALS MODE BELOW
    # zUser, zPass, noteID, tUser, tPass, tProj, ver = askInfo()
    zUser = '' # zeppelin username
    zPass = '' # zeppelin password
    noteID = '' # zeppelin note ID
    tUser = '' # tableau username
    tPass = '' # tableau password
    tProj = '' # tableau destination project 
    ver = '' #tableau API version

    print('\nBeginning Zeppelin queries')

    ### clear old outputs
    print('\nClearing old results')
    clearOutput = 'https://ZEPPELIN_SERVER/zeppelin/api/notebook/' + noteID + '/clear'
    requests.put(clearOutput, auth=HTTPBasicAuth(zUser, zPass))

    ### restart interpreter to allow higher setting for output limit
    print('\nRestarting Interpreter')
    requests.put('https://ZEPPELIN_SERVER/zeppelin/api/interpreter/setting/restart/spark', auth=HTTPBasicAuth(zUser, zPass))
    
    ### run all paragraphs
    print('\nRunning all Paragraphs')
    array = getResults(noteID,zUser, zPass)

    ### Data can be saved to a csv if desired. Set save to y to set this option. Un-comment the two lines below and comment out save = 'n' to ask the user each time.
    #ctypes.windll.user32.MessageBoxW(0, "Zeppelin Queries Complete", "", 0x00001000)
    #save = input('Do you want to save to CSV? y/n')
    save = 'n'
    if save in ['y','Y']:
        # create folder with name current date and time
        print('\nCreating new Folder')
        now = dt.datetime.now()
        folderName = now.strftime("%b_%d_%Y_%H.%M.%S")
        os.mkdir(folderName)

        # save data as csv
        print('\nSaving data as CSV in new folder')
        export(array, folderName)

    ### append to tableau data source
    print('\033[1m' + '\n Beginning Tableau Append Process')

    ### initialization
    print('\nInitializing')
    dest_server = 'http://TABLEAU_SERVER/'
    dest_username = tUser
    dest_site = ''
    dest_password = tPass
    version = ver
    
    ### sign in
    print("\nSigning in to obtain authentication tokens")
    dest_auth_token, dest_site_id, dest_user_id = sign_in(dest_server, dest_username, dest_password)

    ### find project id for destination server
    print("\nFinding project id for {0}".format(dest_server))
    dest_project_id = get_default_project_id(dest_server, dest_auth_token, dest_site_id, version, tProj)

    for i in range(len(array)):
        appendTableau(array[i], dest_server, dest_auth_token, dest_site_id, dest_project_id, version)

    ### sign out
    print("\nSigning out and invalidating the authentication token")
    sign_out(dest_server, dest_auth_token, version)

    ### allows windows to sleep again
    if osSleep:
        osSleep.uninhibit()

    ### popup window upon completion
    ctypes.windll.user32.MessageBoxW(0, "Tableau Datasource Uploads Complete", "", 0x00001000)

    exit()

if __name__ == "__main__":
    main()