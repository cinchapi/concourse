#!/usr/local/bin/python
import time
import base64
import urllib
from oauth2client.service_account import ServiceAccountCredentials

# timestamp describing expiration date of the singed URL
# current Unix Epoch time + 30 days
DAY = 86400 # number of seconds
EXPIRATION = time.time() + 30 * DAY
BASE_URL = 'https://google.com/'
HTTP_METHOD = 'GET'
OBJECT_PATH = 'cinchapi/natural-language-translator/numberbatch-600d.db.tar.gz'
GOOGLE_ACCESS_STORAGE_ID = 'gradle@cinchapi.iam.gserviceaccount.com'
# singature string to be signed
SIGNATURE_STRING = HTTP_METHOD + '\n' \
                   + str(int(EXPIRATION)) + '\n' \
                   + OBJECT_PATH + '\n'

creds = ServiceAccountCredentials.from_json_keyfile_name("id_rsa.json")
client_id = creds.service_account_email
signature = creds.sign_blob(SIGNATURE_STRING)[1]
# url encoded signature
URL_ENCODED_SIGNATURE = urllib.quote_plus(base64.b64encode(signature))

SIGNED_URL = BASE_URL \
             + "?GoogleAccessId=" + GOOGLE_ACCESS_STORAGE_ID \
             + "&Expires=" + str(int(EXPIRATION)) \
             + "&Signature=" + URL_ENCODED_SIGNATURE

f = open('concourse-server/src/main/resources/singed_url','w')
f.write(SIGNED_URL+'\n')
f.close()