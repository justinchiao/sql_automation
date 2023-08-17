#connect_with_tableau

import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import math
import os
import re
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata
import datetime as dt


# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
xmlns = {'t': 'http://tableau.com/api'}

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB
# For when a data source is over 64MB, break it into 5MB (standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5    # 5MB

class ApiCallError(Exception):
    pass

class UserDefinedFieldError(Exception):
    pass

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

def sign_in(server, username, password, version, site=""):
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
    url = server + "/api/{0}/auth/signin".format(version)

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

def get_default_project_id(server, auth_token, site_id, project_name, version):
    """
    Returns the project ID for the 'default' project on the Tableau server.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    """
    page_num, page_size = 1, 100  # default paginating values

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

    ### look through all projects to find the 'default' one (EN and DE locales)
    for project in projects:
        if project.get('name') == project_name:
            return project.get('id')
    print("\tProject was not found in {0}".format(server))

def get_datasource_id(server, auth_token, site_id, datasource_name, version):
    """
    Gets the id of the desired data source to relocate.
    """
    url = server + "/api/{0}/sites/{1}/datasources".format(version, site_id)
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))

    datasources = xml_response.findall('.//t:datasource', namespaces=xmlns)
    for datasource in datasources:
        if datasource.get('name') == datasource_name:
            return datasource.get('id')
    error = "Data source named '{0}' not found.".format(datasource_name)
    raise LookupError(error)

def download(server, auth_token, site_id, datasource_id,version):
    """
    Downloads the desired data source from the server (temp-file).
    """
    print("\tDownloading data source to an archive file")
    url = server + "/api/{0}/sites/{1}/datasources/{2}/content".format(version, site_id, datasource_id)
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)

    # Header format: Content-Disposition: name="tableau_datasource"; filename="datasource-filename"
    now = dt.datetime.now()
    currentDir = 'CURRENT_DIRECTORY_PATH'
    filename = re.findall(r'filename="(.*)"', server_response.headers['Content-Disposition'])[0][:-5]
    folder = currentDir + filename
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = currentDir + filename + '/' + filename + '-' + now.strftime("%Y-%m-%d_%H-%M-%S") + '.tdsx'
    with open(path, 'wb') as f:
        f.write(server_response.content)

def publish_datasource(server, auth_token, site_id, datasource_filename, dest_project_id, version):
    """
    Publishes the data source to the desired project.
    """
    datasource_name, file_extension = datasource_filename.split('.', 1)
    datasource_size = os.path.getsize(datasource_filename)
    chunked = datasource_size >= FILESIZE_LIMIT

    datasource_id = get_datasource_id(server, auth_token, site_id, datasource_name, version)
    download(server, auth_token, site_id, datasource_id, version)

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
        publish_url += "&datasourceType={0}&overwrite=true".format(file_extension) #change append and overwrite here
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
        publish_url += "?datasourceType={0}&overwrite=true".format(file_extension) #change append and overwrite here

    ## make the request to publish and check status code
    print("\tUploading...")
    server_response = requests.post(publish_url, data=payload,
                                    headers={'x-tableau-auth': auth_token, 'content-type': content_type})
    _check_status(server_response, 201)

def main():
    ### initialization
    dest_server = 'TABLEAU_SERVER'
    dest_username = 'TABLEAU_USERNAME'
    dest_site = ''
    dest_password = 'TABLEAU_PASSWORD'
    datasource_filename = 'HYPER_OR_TDSX_FILE'
    project_name = 'TABLEAU_PROJECT_NAME'
    version = '3.2'

    ### sign in
    print("\nSigning in to both sites to obtain authentication tokens")
    # Destination server (site "KonstantinsLiebewiese")
    dest_auth_token, dest_site_id, dest_user_id = sign_in(dest_server, dest_username, dest_password, version, dest_site)
    #print(dest_auth_token)
    #print(dest_site_id)
    
    ### find project id for destination server
    print("\nFinding 'default' project id for {0}".format(dest_server))
    dest_project_id = get_default_project_id(dest_server, dest_auth_token, dest_site_id, project_name, version)
    #print(dest_project_id)

    ### publish to new site
    print("\nPublishing data source to {0}".format(dest_server))
    #print(dest_auth_token)
    publish_datasource(dest_server, dest_auth_token, dest_site_id, datasource_filename, dest_project_id, version)

    ### sign out
    print("\nSigning out and invalidating the authentication token")
    sign_out(dest_server, dest_auth_token, version)


if __name__ == "__main__":
    main()
