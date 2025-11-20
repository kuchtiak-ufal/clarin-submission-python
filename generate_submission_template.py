# This software is licenced under the BSD 3-Clause licence
# available at https://opensource.org/licenses/BSD-3-Clause
# and described in the LICENCE file in the root of this project

"""
Python 3 application for Submission Template Generation, using the dspace.py API client library.
"""
import argparse
import os

from rest_client.submission_client import SubmissionClient

# Example system variables needed for authentication and submission template generation
# (all of these variables can be overwritten with command line arguments)
# AUTHORIZATION_TOKEN=
# DSPACE_API_ENDPOINT=
# SUBMISSION_DEFINITION_NAME=

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Command-line arguments")
parser.add_argument("filename", help="CSV template file name")
parser.add_argument("-t", "--token", help="Authorization token (optional), "
                                "or use the AUTHORIZATION_TOKEN env variable")
parser.add_argument("-e", "--dspace-api-endpoint", help="DSpace API Endpoint (optional), "
                                          "or use the DSPACE_API_ENDPOINT env variable")
parser.add_argument("-s", "--submission-definition-name", help="Submission Definition Name(optional), "
                                             "or use the SUBMISSION_DEFINITION_NAME env variable")
args = parser.parse_args()

AUTHORIZATION_TOKEN = None
if args.token:
    AUTHORIZATION_TOKEN = args.token
elif 'AUTHORIZATION_TOKEN' in os.environ:
    AUTHORIZATION_TOKEN = os.environ['AUTHORIZATION_TOKEN']

if AUTHORIZATION_TOKEN is None:
    print('No authorization token provided!')
    exit(1)

SUBMISSION_DEFINITION_NAME = 'traditional'
if args.submission_definition_name:
    SUBMISSION_DEFINITION_NAME = args.submission_definition_name
elif 'SUBMISSION_DEFINITION_NAME' in os.environ:
    SUBMISSION_DEFINITION_NAME = os.environ['SUBMISSION_DEFINITION_NAME']

API_ENDPOINT = 'http://localhost:8080/server/api'
if args.dspace_api_endpoint:
    API_ENDPOINT = args.dspace_api_endpoint
elif 'DSPACE_API_ENDPOINT' in os.environ:
    API_ENDPOINT = os.environ['DSPACE_API_ENDPOINT']

FILE_TYPE = 'csv'

d = SubmissionClient(api_endpoint=API_ENDPOINT, authorization_token=AUTHORIZATION_TOKEN)

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print('Error logging in! Giving up.')
    exit(1)

# for now, only CSV templates are generated
if FILE_TYPE == 'csv':
    d.generateCsvTemplate(args.filename, SUBMISSION_DEFINITION_NAME)
