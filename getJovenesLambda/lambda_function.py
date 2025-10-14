import json
import pymysql
import os
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import date, datetime
from decimal import Decimal

# Logger configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
secrets_manager_client = boto3.client('secretsmanager')
kms_client = boto3.client('kms')

# Environment variables
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME')
KMS_KEY_ID = os.environ.get('KMS_KEY_ID')

# Database connection cache
db_connection = None

# CORS Headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json'
}

def decimal_date_handler(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def decrypt_data(encrypted_data):
    """Decrypt data using KMS."""
    if not encrypted_data:
        return None
    try:
        import base64
        ciphertext_blob = base64.b64decode(encrypted_data)
        response = kms_client.decrypt(CiphertextBlob=ciphertext_blob)
        return response['Plaintext'].decode('utf-8')
    except Exception as e:
        logger.error(f"KMS decryption error: {e}")
        return None

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
    
    try:
        if db_connection and db_connection.open:
            db_connection.ping(reconnect=True)
            return db_connection
    except:
        db_connection = None
    
    try:
        creds = get_db_credentials()
        db_name = creds.get('database', 'beneficioJoven')
        
        db_connection = pymysql.connect(
            host=creds['host'],
            user=creds['username'],
            password=creds['password'],
            database=db_name,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info(f"Database connection successful to: {db_name}")
        return db_connection
    except pymysql.MySQLError as e:
        logger.error(f"Database connection error: {e}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler for listing jóvenes."""
    
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
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        search = query_params.get('search', '').strip()
        page = int(query_params.get('page', 1))
        limit = int(query_params.get('limit', 10))
        
        # Calculate offset for pagination
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        
        # Build query
        if search:
            # Search by name, folio, or email
            sql = """
                SELECT * FROM vw_jovenes_list
                WHERE nombre_completo LIKE %s
                   OR folio LIKE %s
                   OR correo LIKE %s
                LIMIT %s OFFSET %s
            """
            search_param = f"%{search}%"
            
            # Count total for pagination
            count_sql = """
                SELECT COUNT(*) as total FROM vw_jovenes_list
                WHERE nombre_completo LIKE %s
                   OR folio LIKE %s
                   OR correo LIKE %s
            """
            
            with conn.cursor() as cursor:
                cursor.execute(count_sql, (search_param, search_param, search_param))
                total = cursor.fetchone()['total']
                
                cursor.execute(sql, (search_param, search_param, search_param, limit, offset))
                jovenes = cursor.fetchall()
        else:
            # Get all jóvenes with pagination
            sql = "SELECT * FROM vw_jovenes_list LIMIT %s OFFSET %s"
            count_sql = "SELECT COUNT(*) as total FROM vw_jovenes_list"
            
            with conn.cursor() as cursor:
                cursor.execute(count_sql)
                total = cursor.fetchone()['total']
                
                cursor.execute(sql, (limit, offset))
                jovenes = cursor.fetchall()
        
        # Decrypt phone numbers for all jóvenes
        for joven in jovenes:
            if joven.get('telefono'):
                joven['telefono'] = decrypt_data(joven['telefono'])
        
        # Calculate pagination info
        total_pages = (total + limit - 1) // limit  # Ceiling division
        
        logger.info(f"Retrieved {len(jovenes)} jóvenes (page {page} of {total_pages})")
        
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'data': jovenes,
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                }
            }, default=decimal_date_handler, ensure_ascii=False)
        }
    
    except ValueError as e:
        logger.error(f"Invalid parameter: {e}")
        return {
            'statusCode': 400,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Parámetros inválidos',
                'error': str(e)
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error de base de datos',
                'error': str(e)
            })
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error interno del servidor',
                'error': str(e)
            })
        }