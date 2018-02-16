
#######################################################################################################################
#
#   SalesForce Ticket Analysis 
#
#   Developed in Python 3.6.4
#
#######################################################################################################################


import sys, os, re, csv
import datetime, time
import json
from simple_salesforce import Salesforce
import requests
import math
import pysolr


#######################################################################################################################
#
#   Functions - General
#
#######################################################################################################################

def write_to_disk(object_to_write, filepathname):
    try:
        file = open(filepathname,'wb')
        file.write(json.dumps(object_to_write))
        file.close()
        print('[ INFO ] JSON object written to ' + str(filepathname))
    except:
        print('[ ERROR ] Issue writing object to disk. Check object name and file path/name')


def read_from_disk(filepathname):
    try:
        file = open(filepathname,'rb')
        print('[ INFO ] JSON object read from ' + str(filepathname))
        return json.loads(file.read())
    except:
        print('[ ERROR ] Issue reading object to disk. Check object name and file path/name')


def cleanup_utf8_chars(input_string):
    try:
        return re.sub('(\r|\n|\t)',' ',re.sub(r'[^\x00-\x7F]+',' ', input_string)).strip()
    except:
        return input_string


def count_categories(text, term_list):
    return len(re.findall('('+'|'.join(term_list)+')',text,re.IGNORECASE))


def dedup(list):
    seen = set()
    out  = []
    for i in list:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


#######################################################################################################################
#
#   Connect to SF (Authentication)
#
#######################################################################################################################

def connect_to_sf(username=us, password=pw, security_token=tk):
    if password == '':
        password = raw_input('Enter Password: ')
    
    try:
        sf = Salesforce(username=username, password=password, security_token=security_token)
        print('[ INFO ] - Successfully connected to SF')
        return sf
    except:
        print('[ ERROR ] - Issue connecting to SF. Check username, password, and verify that token is correct.')
        sys.exit()


def print_all_available_objects():
    for x in sf.describe()["sobjects"]:
        if re.search('activ', x["label"].lower()):
            print(x["label"])


#######################################################################################################################
#
#   SF Accounts
#
#######################################################################################################################

def get_sf_accounts(print_accounts=False):
    '''
    https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_account.htm
    '''
    accounts = {}
    
    def parse_record(record):
        recordid                = record['Id']
        url                     = record['attributes']['url']
        name                    = record['Name']
        date_last               = record['LastActivityDate']
        AccountSource           = record['AccountSource']
        Industry                = record['Industry']
        IsPartner               = record['IsPartner']
        BillingPostalCode       = record['BillingPostalCode']
        BillingState            = record['BillingState']
        BillingCountry          = record['BillingCountry']
        return recordid, name, date_last
    
    # Query SF object and paginate through results
    accts = sf.query("select id, name, LastActivityDate, AccountSource, Industry, IsPartner, BillingPostalCode, BillingState, BillingCountry from account")
    accts_count = accts['totalSize']
    print('[ INFO ] Total Number of Accounts: ' + str(accts_count))
    
    pagination_count = int(math.ceil(accts_count / float(2000)))
    
    for page in range(pagination_count):
        
        if page == 0:
            for record in accts['records']:
                try:
                    acct_record = parse_record(record)
                    accounts[acct_record[0]] = {'name':acct_record[1], 'date_last':acct_record[2]}
                    if print_accounts: print(acct_record)
                except:
                    print('[ WARNING ] Passed on ' + str(record))
        else:
            nextRecordsUrl = accts['nextRecordsUrl']
            accts = sf.query_more(nextRecordsUrl, True)
            for record in accts['records']:
                try:
                    acct_record = parse_record(record)
                    accounts[acct_record[0]] = {'name':acct_record[1], 'date_last':acct_record[2]}
                    if print_accounts: print(acct_record)
                except:
                    print('[ WARNING ] Passed on ' + str(record))
    
    return accounts


def get_account_name(account_id):
    return accounts[account_id]['name']


def get_account_id(account_name):
    return [(k,v) for k,v in accounts.iteritems() if re.search(account_name.lower(),v['name'].lower())]


#######################################################################################################################
#
#   SF Tickets
#
#######################################################################################################################

def get_all_closed_tickets():
    '''
    https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_case.htm
    [i['name'] for i in sf.__getattr__('Case').describe()['fields']]
    '''
    print('[ INFO ] Getting all closed tickets within SF...')
    
    def parse_record(record):
        url                     = record['attributes']['url']
        AccountId               = record['AccountId']
        try:
            account_name        = get_account_name(AccountId)
        except:
            account_name        = 'Unknown, AccountID = ' + str(AccountId)
        CaseNumber              = record['CaseNumber']
        SuppliedName            = record['SuppliedName']
        SuppliedEmail           = record['SuppliedEmail']
        ContactId               = record['ContactId']
        CreatedDate             = record['CreatedDate']
        LastModifiedDate        = record['LastModifiedDate']
        SLA_DueDate__c          = record['SLA_DueDate__c']
        Description             = record['Description']
        Problem_Question        = record['Problem_Statement_Question__c']
        Status_Resolution       = record['Current_Status_Resolution__c']
        Root_Cause              = record['Root_Cause__c']
        Reason                  = record["Reason"]
        Status                  = record["Status"]
        Priority                = record["Priority"]
        Problem_Type__c         = record["Problem_Type__c"]
        return {'account_name':account_name, 'CaseNumber':CaseNumber, 'SuppliedName':SuppliedName, 'SuppliedEmail':SuppliedEmail, 'ContactId':ContactId, 'CreatedDate':CreatedDate, 'LastModifiedDate':LastModifiedDate, 'SLA_DueDate__c':SLA_DueDate__c, 'Description':Description, 'Problem_Question':Problem_Question, 'Status Resolution':Status_Resolution, 'Root_Cause':Root_Cause, 'Reason':Reason, 'Status':Status, 'Priority':Priority, 'Problem_Type__c':Problem_Type__c}
        #return {'AccountId':AccountId, 'CaseNumber':CaseNumber}
    
    # Query SF to get all closed tickets
    tickets = sf.query("select accountid, casenumber, SuppliedName, SuppliedEmail, ContactId, CreatedDate, LastModifiedDate, SLA_DueDate__c, description, Problem_Statement_Question__c, Current_Status_Resolution__c, Root_Cause__c, reason, status, priority, problem_type__c from case where isclosed=true order by CreatedDate desc")
    tickets_count = tickets['totalSize']
    print('[ INFO ] Total Number of Tickets: ' + str(tickets_count))
    
    results_per_url  = len(tickets['records']) # Ive found that this value is usually 2000 or 500 depending on the SF object.
    pagination_count = int(math.ceil(tickets_count / float(results_per_url)))
    
    parsed_records = []
    
    for page in range(pagination_count):
        
        if page == 0:
            for record in tickets['records']:
                #print(parse_record(record))
                parsed_records.append(parse_record(record))
        else:
            nextRecordsUrl = tickets['nextRecordsUrl']
            tickets = sf.query_more(nextRecordsUrl, True)
            for record in tickets['records']:
                #print(parse_record(record))
                parsed_records.append(parse_record(record))
    
    print('[ INFO ] Collected ' + str(len(parsed_records)) + ' total tickets')
    return parsed_records


#######################################################################################################################
#
#   POST to Solr
#
#   https://github.com/django-haystack/pysolr
#
#######################################################################################################################

def connect_to_zookeeper(zkhosts='localhost:2181'):
    '''
    zookeeper = connect_to_zookeeper(zkhosts='localhost:2181')
    '''
    #pysolr.ZooKeeper("zkhost1:2181,zkhost2:2181,zkhost3:2181")
    return pysolr.ZooKeeper(zkhosts)


def connect_to_solrcloud(collection):
    '''
    solr = connect_to_solrcloud('my_collection')
    '''
    return pysolr.SolrCloud(zk, collection)
    print('[ INFO ] Connected to Solr cloud (collection = ' + str(collection) + ')')


def connect_to_solr(collection):
    '''
    solr = connect_to_solr('my_collection') 
    '''
    return pysolr.Solr('http://localhost:8983/solr/' + str(collection), timeout=10)
    print('[ INFO ] Connected to Solr')


def add_to_solr(doc_id, problem_question, description, root_cause, resolution, status_resolution, problem_type, technology):
    try:
        solr.add([
            {
                "id": doc_id,
                "problem_question": problem_question,
                "description": description,
                "root_cause": root_cause,
                "resolution": resolution,
                "status_resolution": status_resolution,
                "problem_type": problem_type,
                "technology": technology
            }
        ])
        print('[ INFO ] Successfully added doc_id = ' + str(doc_id))
    except:
        print('[ ERROR ] Could not write doc_id = ' + str(doc_id))


#######################################################################################################################
#
#   Product Tech Categories
#
#######################################################################################################################

def product_category(text, number_of_results=3):
    #text = text.decode('utf-8')
    category_map =  {
                        "nifi":['nifi'],
                        "minifi":['minifi'],
                        "storm":['storm'],
                        "kafka":['kafka'],
                        "streaming analytics manager":['streaming analytic',' sam ','stream processing','real-time','realtime'],
                        "schema registry":['schema registry','schemaregistry'],

                        "ambari":['amabari','operations'],
                        "ranger":['ranger','security'],
                        "knox":['knox','proxy','gateway'],
                        "atlas":['atlas','governance','lineage'],
                        "zookeeper":['zookeeper'],

                        "hdfs":['hdfs'],
                        "yarn":['yarn','resource manager','resourcemanager'],
                        "mapreduce":['mapreduce'],
                        "hive":['hive','sql',' tez ','llap'],
                        "hbase":['hbase','phoenix','region server','region master'],
                        "sqoop":['sqoop','bulkload'],
                        "oozie":['oozie'],
                        "spark":['spark','data science','machine learning','deep learning','analytic'],
                        "zeppelin":['zeppelin','data science','machine learning','analytic','notebook','code editor'],
                        "druid":['druid','olap'],
                        "solr":['solr','search and indexing','indexing','search'],

                        "smartsense":['smartsense','smart sense'],

                        "metron":['metron','cybersecurity'],

                        "cloudbreak":['cloudbreak'],

                        "data lifecycle manager":['data lifecyle manager','dataplane','dlm'],
                        "data steward studio":['data steward studio','dataplane','dss'],
                        "data analytics studio":['data analytics studio','dataplane','das'],

                        "data science experience":['data science experience','dsx','data science','machine learning','rstudio','jupyter','model management','model deployment'],
                        "bigsql":['bigsql','big sql','ansi sql','edw offload','edw migration'],
                        
                        "professional services":['professional service',' ps '],

                        "techinical support":['support','subscription']
                    }
    
    category_matches = {}
    values = []
    for k,v in category_map.items():
        value = count_categories(text, v)
        category_matches[k] = value
        values.append(value)
    
    if max(values) == 0:
        return ''
    else:
        category_matches_standardized = [(k,v/float(max(values))*100) for k,v in category_matches.items()]
        #return {"results": [i for i in sorted(category_matches_standardized, key=lambda (k,v): v, reverse=True) if i[1]>0][:number_of_results] }
        return ', '.join([i[0] for i in sorted(category_matches_standardized, key=lambda item: item[1], reverse=True) if i[1]>0][:number_of_results])


def get_rootcause(status_resolution, result_found=0):
    match = re.findall('root[ ]*cause.*?resolution', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('root[ ]*cause','',re.sub('resolution','',match[0], flags=re.IGNORECASE), flags=re.IGNORECASE).replace(':',' ').strip()
    
    match = re.findall('root[ ]*cause.*?resolut', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('root[ ]*cause','',re.sub('resolut','',match[0], flags=re.IGNORECASE), flags=re.IGNORECASE).replace(':',' ').strip()
    
    match = re.findall('root[ ]*cause.*?solution', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('root[ ]*cause','',re.sub('solution','',match[0], flags=re.IGNORECASE), flags=re.IGNORECASE).replace(':',' ').strip()
    
    match = re.findall('root[ ]*cause.+', status_resolution, flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('root[ ]*cause','',match[0], flags=re.IGNORECASE).replace(':',' ').strip()
    
    if result_found == 0:
        return ''


def get_resolution(status_resolution, result_found=0):
    match = re.findall('resolution.+', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('resolution','',match[0], flags=re.IGNORECASE).replace(':',' ').strip()
    
    match = re.findall('resolut.+', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return match[0].strip()
    
    match = re.findall('solution.+', re.sub('(\n|\r)','<br>', status_resolution), flags=re.IGNORECASE)
    if len(match) != 0:
        result_found = 1
        return re.sub('solution','',match[0], flags=re.IGNORECASE).replace(':',' ').strip()
    
    if result_found == 0:
        return ''


#######################################################################################################################
#
#   Main
#
#######################################################################################################################

if __name__ == '__main__':
    
    # Authenticate SF
    sf   = connect_to_sf(username=us, password=pw, security_token=tk)
    # Connect Zookeeper
    zk   = connect_to_zookeeper(zkhosts='localhost:2181') 
    # Connect Solr
    #solr = connect_to_solrcloud(collection='hwx_search')
    solr = connect_to_solr(collection='hwx_search')

    # Get Account IDs and associated names
    accounts = read_from_disk('/Users/dzaratsian/Desktop/sf/accounts.json')
    #accounts = get_sf_accounts(print_accounts=False)

    # Get all closed tickets
    closed_tickets = read_from_disk('/Users/dzaratsian/Desktop/sf/tickets.json')
    #closed_tickets = get_all_closed_tickets()

    batch_size  = 1000
    batch_start = 0
    batch       = []

    for i,ticket in enumerate(closed_tickets):

        if i <= 5000:

            batch_start = batch_start
            batch_end   = batch_start + batch_size

            try:
                doc_id              = ticket['CaseNumber']
                problem_question    = ticket['Problem_Question']
                description         = ticket['Description']
                status_resolution   = ticket['Status Resolution']
                problem_type        = ticket['Problem_Type__c']

                blob                = str(problem_question) + ' --- ' + str(description) + ' --- ' + str(ticket['Root_Cause']) + ' --- ' + str(status_resolution)

                technology          = product_category(blob)

                root_cause          = get_rootcause(status_resolution)
                if root_cause == '':
                    root_cause      = ticket['Root_Cause']

                resolution          = get_resolution(status_resolution)

                batch.append(
                    {
                        "id": doc_id,
                        "problem_question": problem_question,
                        "description": description,
                        "root_cause": root_cause,
                        "resolution": resolution,
                        "status_resolution": status_resolution,
                        "problem_type": problem_type,
                        "technology": technology
                    })
                print('[ INFO ] Insert success (index = ' + str(i) + ') ') #+ str(ticket))
            except:
                print('[ ERROR ] Passed on (index = ' + str(i) + ') ') #+ str(ticket))

            if i == batch_end:
                # Bulkload batch to Solr
                solr.add(batch)
                print('[ INFO ] Bulkloaded ' + str(len(batch)) + ' records (batch_index = ' + str(batch_end) + ')' )
                # Update batch start
                batch_start = batch_end + 1
                batch       = []




'''
##############################################################################

solr.delete(q='*:*')


##############################################################################
'''




#ZEND
