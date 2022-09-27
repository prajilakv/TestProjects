import requests, time, os
import urllib.request
import logging 

import xml.etree.ElementTree as et
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
import boto3


#log
location = os.path.join("C:\Test",time.strftime("%Y%m%d%H%M%S")) 
os.mkdir(location)
logfilename = os.path.join(location,'logfilexml.log')
print(logfilename)
#logging.basicConfig(level = logging.INFO,filename=logfilename,format='%(asctime)s %(message)s',filemode='w')
logging.basicConfig(filename = logfilename, level = logging.DEBUG,format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s',force=True)

def log_file_writing(message,logtype):
    if logtype == 'info':
        logging.info(message)
    elif logtype == 'error':
        logging.error(message)
            

try:

    #Function to download zip file from the url
    def download_zip(zip_url,save_to_path):
        try:
            with urllib.request.urlopen(zip_url) as zip_file:
                with ZipFile(BytesIO(zip_file.read())) as zfile:
                    zfile.extractall(save_to_path) 
                    log_file_writing('Zip file downloaded succesfully','info')
        except Exception as e:
            log_file_writing('Zip file download failed','error')
            log_file_writing(e,'error')

    # function to return xml file path from the directory        
    def get_xml_file_from_folder(path):
        try:
            for filename in os.listdir(path):
                if filename.endswith('.xml'):
                    source_file = os.path.join(path, filename)
                    break
            log_file_writing('Got source file','info')        
            return source_file
        except Exception as e:
            log_file_writing(e,'error')

    #function to save file
    def save_file(file_path,to_save):
        try:
            with open(file_path, 'w+') as file:
                file.write(to_save.text)
            log_file_writing('File Saved','info')
        except Exception as e:
            log_file_writing(e,'error')    

    
    webpage = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100' 
    try:
        response = requests.get(webpage)
        log_file_writing('Got response.....','info')
    except Exception as e:
        log_file_writing(e,'error') 
    
    #Get source xml file from the first url
    filename = location+'\webxml.xml'
    save_file(filename,response) 
    
    xml_parsed = et.parse(filename)
    root = xml_parsed.getroot()
    result = root.find("result")
    download_link = ''
    
    try:
        for doc in result.findall("doc"):
            if doc.find('*[@name = "file_type"]').text == 'DLTINS':
                download_link = doc.find('*[@name = "download_link"]').text
            if download_link != '':
                break 
        download_zip(download_link,location)
        source = get_xml_file_from_folder(location)
        log_file_writing('Source File Saved succesfully','info')
    except Exception as e:
        log_file_writing(e,'error') 


    
    #Parsing source xml and create csv
    try:

        dict_list = []
        for _, elem in et.iterparse(source, events=("end",)):

            #print(elem)
            prefix, has_namespace, remainder = elem.tag.partition("}")
            if has_namespace:
                namespace, tag = prefix[1:], remainder

            else:
                namespace, tag = None, prefix
            childpref = prefix + has_namespace     
            if tag == 'TermntdRcrd':
                child_elem = elem.find(childpref+'FinInstrmGnlAttrbts')
                dict_item = {'FinInstrmGnlAttrbts.Id': child_elem.find(childpref+'Id').text,
                                  'FinInstrmGnlAttrbts.FullNm': child_elem.find(childpref+'FullNm').text,
                                  'FinInstrmGnlAttrbts.ClssfctnTp': child_elem.find(childpref+'ClssfctnTp').text,
                                  'FinInstrmGnlAttrbts.CmmdtyDerivInd': child_elem.find(childpref+'CmmdtyDerivInd').text,
                                  'FinInstrmGnlAttrbts.NtnlCcy': child_elem.find(childpref+'NtnlCcy').text}

                dict_item['Issr'] = elem.find(childpref+'Issr').text

                dict_list.append(dict_item)
        log_file_writing('Data Extracted sucesfully','info')
    except Exception as e:
        log_file_writing(e,'error')     


    # Create pandas dataframe
    header=['FinInstrmGnlAttrbts.Id','FinInstrmGnlAttrbts.FullNm','FinInstrmGnlAttrbts.ClssfctnTp',
            'FinInstrmGnlAttrbts.CmmdtyDerivInd','FinInstrmGnlAttrbts.NtnlCcy','Issr']
    df = pd.DataFrame(dict_list, columns=header)
    #df.head()

    #Create CSV
    df.to_csv(location+'\output.csv')
    
    log_file_writing('Process Completed!','info')
except Exception as e:
    log_file_writing(e,'error')
    print(e)
    
#Upload to AWS S3


try:
    
    ACCESS_KEY = ''#enter the aws acces key
    SECRET_KEY = ''# enter the aws secret key


    def upload_to_aws(local_file, bucket, s3_file):
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

        try:
            s3.upload_file(local_file, bucket, s3_file)
            log_file_writing('uploaded!','info')
        except Exception as e:
            log_file_writing(e,'error')

    my_bucket = ''#Enter bucket name here
    uploaded = upload_to_aws(location+'\output.csv', my_bucket, 'output.csv')
    log_file_writing('Upload Completed!','info')
except Exception as e:
    log_file_writing(e,'error')

