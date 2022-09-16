#!/usr/bin/python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Inserts an offline conversion attributed to an encrypted user ID."""

import argparse
import sys
import time

import dfareporting_utils
from oauth2client import client

import pdb

import httplib2
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

import logging
import contextlib
from http.client import HTTPConnection  # py3

path_to_private_key_file = 'C:\\Users\\dh211987\\dh-media-dev-49348-a47ed3a61b26.json'

API_NAME = 'dfareporting'
API_VERSION = 'v4'
API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions']

profile_id = '5654144'
encrypted_user_id = "acc56602-2349-4fc5-8b04-d679af6366a8"
floodlight_activity_id = 13280814

def main(argv):
  
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    #"dh-media-dev-49348-a47ed3a61b26.json",
    # scopes="https://www.googleapis.com/auth/dfareporting"
    path_to_private_key_file,
    scopes=API_SCOPES,
    )

    http = credentials.authorize(httplib2.Http())

    # Construct a service object via the discovery service.
    service = discovery.build('dfareporting', 'v4', http=http)

    try:
        # Look up the Floodlight configuration ID based on activity ID.
        floodlight_activity = service.floodlightActivities().get(
            profileId=profile_id, id=floodlight_activity_id).execute()
        #pdb.set_trace()
        floodlight_config_id = floodlight_activity['floodlightConfigurationId']
        print(floodlight_config_id)
        
        current_time_in_micros = int(time.time() * 1000000)
        
        convertions = []

        # Construct the conversion.
        conversion = {
            'encryptedUserId': encrypted_user_id,
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_config_id,
            'ordinal': current_time_in_micros,
            'timestampMicros': current_time_in_micros,
            'quantity': 1,
            "customVariables": [
                {
                    "kind": "dfareporting#customFloodlightVariable",
                    "type": "U2",
                    "value": encrypted_user_id
                }
            ]
        }
        
        convertions.append(conversion)

#     # Construct the encryption info.
#     encryption_info = {
#         'encryptionEntityId': encryption_entity_id,
#         'encryptionEntityType': encryption_entity_type,
#         'encryptionSource': encryption_source,
#  #       'kind': "dfareporting#encryptionInfo"
#     }

        # Insert the conversion.
        request_body = {
            #'conversions': [conversion],
            # 'encryptionInfo': encryption_info
            'conversions': [conversion],
        }
        request = service.conversions().batchinsert(profileId=profile_id,
                                                    body=request_body)
        print(request_body)
        response = request.execute()
        print(response)

        if not response['hasFailures']:
            print ('Successfully inserted conversion for encrypted user ID %s.'
                % encrypted_user_id)
        else:
            print ('Error(s) inserting conversion for encrypted user ID %s.'
                % encrypted_user_id)

        status = response['status'][0]
        for error in status['errors']:
        # print '\t[%s]: %s' % (error['code'], error['message'])
            print ('\t%s: %s' % (error['code'], error['message']))

    except client.AccessTokenRefreshError:
        print ('The credentials have been revoked or expired, please re-run the '
            'application to re-authorize')


if __name__ == '__main__':
  main(sys.argv)
