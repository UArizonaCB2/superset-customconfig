from urllib.parse import quote_plus
import os

ROW_LIMIT = 100000
PREFERRED_DATABASES = [
    'Amazon Athena'
]

SECRET_KEY=os.environ.get('SECRET_KEY')
if SECRET_KEY is None:
    raise Exception('SECRET_KEY environment variable not set')

def custom_db_connector_mutator(uri, params, username, security_manager, source):

    # We only update the sql alchemy parameters 
    if not uri.host == 'mdh.athena.com': 
        return uri, params
    """
    Rewrite the SQLALCHEMY_DATABASE_URI here if needed for mdh specific injections.
    uri = (
            f'awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}'
            f'@athena.{region_name}.amazonaws.com:443/{schema_name}'
            f'?s3_staging_dir={quote_plus(s3_staging_dir)}&work_group={work_group}&aws_session_token={quote_plus(aws_session_token)}'
            )
    """
    return uri, params

DB_CONNECTION_MUTATOR=custom_db_connector_mutator
