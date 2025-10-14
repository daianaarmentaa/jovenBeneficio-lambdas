import json
import pymysql
import os
import logging
from botocore.exceptions import ClientError
import boto3

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
secrets_manager_client = boto3.client('secretsmanager')

# Environment variables
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME')

# Database connection cache
db_connection = None

# CORS Headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json'
}

def get_db_credentials():
    """Get database credentials from AWS Secrets Manager."""
    try:
        response = secrets_manager_client.get_secret_value(SecretId=DB_SECRET_NAME)
        return json.loads(response['SecretString'])
    except ClientError as e:
        logger.error(f"Error getting DB credentials: {e}")
        raise

def get_db_connection():
    """Establish database connection with connection reuse."""
    global db_connection
    if db_connection and db_connection.open:
        return db_connection
    
    try:
        creds = get_db_credentials()
        db_name = creds.get('database', 'beneficioJoven')
        
        db_connection = pymysql.connect(
            host=creds['host'],
            user=creds['username'],
            password=creds['password'],
            database=db_name,
            connect_timeout=5,
            cursorclass=pymysql.cursors.DictCursor  # Return results as dictionaries
        )
        logger.info(f"Database connection successful to: {db_name}")
        return db_connection
    except pymysql.MySQLError as e:
        logger.error(f"Database connection error: {e}")
        raise

def lambda_handler(event, context):
    """Get all categories."""
    
    logger.info(f"Event received: {json.dumps(event)}")
    
    # Get HTTP method
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    
    # Handle OPTIONS preflight
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'OK'})
        }
    
    try:
        # Get all categories
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "SELECT id_categoria, nombre FROM Categoria ORDER BY nombre ASC"
            cursor.execute(sql)
            categorias = cursor.fetchall()
        
        logger.info(f"Found {len(categorias)} categories")
        
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'categorias': categorias
            })
        }
    
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error al obtener categorías',
                'error': str(e)
            })
        }