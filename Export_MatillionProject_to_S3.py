import requests
import boto3 as boto
import csv
import json
import pandas as pd
from io import StringIO
import io

s3 = boto.client('s3')
s3_r = boto.resource('s3')
bucket = s3_r.Bucket(s3_bucket_kg)
ec2_user = ''
ec2_pass = ''

########CREDENTIALS#######################################
#Download files from S3 to local directory
for key in bucket.objects.all():
  if 'python_instance_creds' in key.key:
    s3dir_s3file = key.key
    s3file = s3dir_s3file
    localdir_s3file = '/tmp/' + s3file
    s3.download_file(s3_bucket_kg, s3dir_s3file, localdir_s3file)
    print('File: '+s3file+ ' saved to: '+ localdir_s3file)

with open('/tmp/python_instance_creds.csv') as csv_file:
  csv_reader = csv.reader(csv_file, delimiter = ',')
  line_count = 0
  for row in csv_reader:
    if line_count == 0:
      ec2_user = str(row)[2:][:-2]
      line_count += 1
    if line_count == 1:
      ec2_pass = str(row)[2:][:-2]                     
  print('EC2 Credentials saved')
      
print('EC2 User: ' + str(ec2_user))
print('EC2 Password: ' + str(ec2_pass))

#######EXPORT########################################
#EXPORT PROJECT TO R
r = requests.get('https://34.248.81.61/rest/v1/group/name/Matillion/project/name/Test/export', 
                 auth=(ec2_user,ec2_pass), 
                 verify=False)

print(r.text)

#CREATE PANDAS DATAFRAME & CONVERT TO JSON
#d = {r}
#df = pd.DataFrame(data=d)
#json_df = df.to_json()

#print(json_df)

#CREATE JSON FILE LOCALLY & UPLOAD TO S3
filename = 'projectexport.json'
with io.open('/tmp/'+filename, 'w', encoding='utf-8') as f:
  #f.write(json.dumps(json_df, ensure_ascii=False))
  f.write(r.text)
  s3.upload_file('/tmp/'+filename, s3_bucket_kg, 'python-boto/'+filename)
