from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode
import csv
import datetime as dt
import string

def isDate(string):
    try:
        t = dt.datetime.strptime(string, "%Y-%m-%d") #"%Y-%m-%d" Zeppelin format 2023-01-31, "%m/%d/%Y" excel format 1/31/2023
        return True, t
    except ValueError as err:
        return False, 'not a date'
    
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def convertToHyper(dictionary, filename):
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp') as hyper:
        # Create the extract, replace it if it already exists
        
        #iso alpha2 country codes
        alpha2 = ['AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BH', 'BS', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA', 'CV', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CG', 'CD', 'CK', 'CR', 'CI', 'HR', 'CU', 'CW', 'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE', 'RO', 'RU', 'RW', 'BL', 'SH', 'KN', 'LC', 'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SZ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU', 'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW']

        #set column name and type and set data values to correct type
        columns = []
        for i in range(len(dictionary['columns'])):
            columnName = dictionary['columns'][i]
            sample = dictionary['data'][20][i].translate(str.maketrans('', '', '$,'))
            print(sample)
            if isfloat(sample): #checks if value is an float
                type = SqlType.double()
                print('double')
                for j in range(len(dictionary['data'])):
                    dictionary['data'][j][i] = float(dictionary['data'][j][i].translate(str.maketrans('', '', '$,')))
            elif True in isDate(sample): #checks if value is a date in the same format as the database
                type = SqlType.date()
                print('date')
                for j in range(len(dictionary['data'])):
                    dictionary['data'][j][i] = isDate(dictionary['data'][j][i])[1]
            elif len(sample) == 2: #checks if value is shorter than 2
                if sample in alpha2: #checks if value is in the list of alpha2 country codes
                    type = SqlType.geography()
                    print('geo')
                    for j in range(len(dictionary['data'])):
                        dictionary['data'][j][i] = bytes(dictionary['data'][j][i], 'utf-8')
            else: 
                print('text')
                type = SqlType.text()
                
            columns.append(TableDefinition.Column(columnName, type)) #add current column to list of columns

        with Connection(hyper.endpoint, filename, CreateMode.CREATE_AND_REPLACE) as connection: #create 
            schema = TableDefinition(filename[:-6], columns)
            connection.catalog.create_table(schema)
            with Inserter(connection, schema) as inserter:
                inserter.add_rows(dictionary['data'])
                inserter.execute()


data = list(csv.reader(open('CSV_FILE','r'))) #source csv filename
dictionary = {}
dictionary['columns'] = data[0]
dictionary['columns'][0] = 'f_timestamp_day'#change to first column name. Encoding issues
dictionary['data'] = data[1:]

convertToHyper(dictionary, filename='HYPER_FILENAME') #.hyper filename

