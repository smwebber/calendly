import os
import json
import boto3
import pandas
import requests
import datetime
from io import StringIO
import logging
import datetime
import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables (Configured in Lambda)
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'smw-calendly-ecr-bucket')
S3_FOLDER_PATH = os.getenv('S3_FOLDER_PATH', 'calendly/')
SECRET_NAME = os.getenv('CALENDLY_SECRET_NAME', 'calendly')
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')

# Initialize AWS Clients
secrets_client = boto3.client(service_name='secretsmanager', region_name=REGION_NAME)
s3_client = boto3.client(service_name='s3', region_name=REGION_NAME)

# Generate Timestamp for file naming
timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
S3_CALENDLY_PATH = f'{S3_FOLDER_PATH}scheduled_calls/{timestamp}.csv'
S3_METRICS_PATH = f'{S3_FOLDER_PATH}metrics/{timestamp}.csv'


def get_calendly_secret():
    """
    Retrieves the Calendly API token from AWS Secrets Manager.
    """
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response['SecretString'])
        logger.info(secret)
        return secret
    except Exception as e:
        logger.error(f'Error retrieving secret: {e}')
        raise


def get_from_calendly(endpoint, params=None):
    """
    Makes a GET request to the Calendly API.
    """
    headers = get_calendly_secret()
    url = f'https://api.calendly.com/{endpoint}' + ('?' + '&'.join([f'{key}={value}' for  key, value in params.items()]) if params else '')

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_current_user():
    """
    Retrieves the current user's information from Calendly.
    """
    return get_from_calendly('users/me')['resource']


def get_current_organization():
    """
    Retrieves the current user's organization information from Calendly.
    """
    return get_current_user()['current_organization']


def get_event_types():
    """
    Retrieves the event types for the current organization.
    """
    organization = get_current_organization()
    return get_from_calendly('event_types', params={'organization': organization})['collection']


def get_scheduled_events():
    """
    Retrieves scheduled events for the current organization.
    """
    all_events = []
    organization = get_current_organization()
    event_types = get_event_types()
    for event_type in event_types:
        scheduled_events = get_from_calendly('scheduled_events', params={'event_type': event_type['uri'], 'organization': organization})
        for event in scheduled_events['collection']:
            start_time = datetime.datetime.strptime(event['start_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
            if event.get(start_time, datetime.datetime(1970, 1, 1)) > datetime.datetime.now():
                continue
            all_events.append({
                'event_id': event.get('uri', ''),
                'name': event.get('name', ''),
                'start_time': event.get('start_time', ''),
                'end_time': event.get('end_time', ''),
                'event_type': event.get('event_type', ''),
                'status': event.get('status', ''),
                'invitees': event.get('invitees_counter', '').get('total', ''),
                'location': str(event.get('location', {}).get('join_url', '')).replace('None', '')
            })

    return pandas.DataFrame(all_events)


def calculate_metrics(df):
    """
    Calculates metrics based on the scheduled events data.
    """
    if df.empty:
        return pandas.DataFrame()

    df['start_time'] = pandas.to_datetime(df['start_time'])
    df['end_time'] = pandas.to_datetime(df['end_time'])
    df['duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

    metrics = df.groupby('name').agg({
        'event_id': 'count',
        'invitees': 'sum',
        'duration': 'mean'
    }).reset_index()

    metrics.columns = ['name', 'scheduled_events', 'total_invitees', 'avg_duration']

    return metrics


def upload_to_s3(df, s3_path):
    if df.empty:
        logger.info('No data to upload to S3.')
        return

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_path,
        Body=csv_buffer.getvalue()
    )

    logger.info(f'Data uploaded to S3: {s3_path}')


def lambda_handler(event, context):
    logger.info('Lambda function started.')

    try:
        scheduled_events = get_scheduled_events()
        upload_to_s3(scheduled_events, S3_CALENDLY_PATH)

        metrics = calculate_metrics(scheduled_events)
        upload_to_s3(metrics, S3_METRICS_PATH)

        logger.info('Lambda function completed successfully.')

        return {
            'statusCode': 200,
            'body': json.dumps('S3 Bucket Successfully updated')
        }
    except Exception as e:
        logger.error(f'Error in lambda_handler: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {e}')
        }

if __name__ == "__main__":
    lambda_handler(None, None)
