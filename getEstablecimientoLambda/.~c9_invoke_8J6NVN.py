import json
import pymysql
import os
import logging
import boto3
import base64
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
        logger.info("No encrypted data provided (NULL or empty)")
        return None
    
    try:
        # Check if it looks like corrupted/truncated data
        if len(encrypted_data) < 20:
            logger.warning(f"Data appears corrupted or truncated: {encrypted_data}")
            return None
        
        logger.info(f"Attempting to decrypt data: {encrypted_data[:30]}...")
        
        # Try to base64 decode
        try:
            ciphertext_blob = base64.b64decode(encrypted_data)
        except Exception as decode_error:
            logger.warning(f"Base64 decode failed: {decode_error}")
            return None
        
        # Try to decrypt with KMS
        try:
            response = kms_client.decrypt(CiphertextBlob=ciphertext_blob)
            decrypted = response['Plaintext'].decode('utf-8')
            logger.info(f"Successfully decrypted data")
            return decrypted
        except Exception as kms_error:
            logger.error(f"KMS decrypt failed: {kms_error}")
            return None
        
    except Exception as e:
        logger.error(f"Decryption error: {e}", exc_info=True)
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
    """Main Lambda handler for listing establecimientos."""
    
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
        order_by = query_params.get('orderBy', 'id')  # id, nombre_establecimiento, categoria, colonia
        order_dir = query_params.get('orderDir', 'ASC').upper()
        
        # Validate order parameters
        valid_order_by = ['id', 'nombre_establecimiento', 'categoria', 'colonia', 'fecha_registro']
        if order_by not in valid_order_by:
            order_by = 'id'
        if order_dir not in ['ASC', 'DESC']:
            order_dir = 'ASC'
        
        # Calculate offset for pagination
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        
        # Build query
        if search:
            # Search by business name, category, or colonia
            sql = f"""
                SELECT * FROM vw_establecimientos_list
                WHERE nombre_establecimiento LIKE %s
                   OR categoria LIKE %s
                   OR colonia LIKE %s
                   OR correo_publico LIKE %s
                ORDER BY {order_by} {order_dir}
                LIMIT %s OFFSET %s
            """
            search_param = f"%{search}%"
            
            # Count total for pagination
            count_sql = """
                SELECT COUNT(*) as total FROM vw_establecimientos_list
                WHERE nombre_establecimiento LIKE %s
                   OR categoria LIKE %s
                   OR colonia LIKE %s
                   OR correo_publico LIKE %s
            """
            
            with conn.cursor() as cursor:
                cursor.execute(count_sql, (search_param, search_param, search_param, search_param))
                total = cursor.fetchone()['total']
                
                cursor.execute(sql, (search_param, search_param, search_param, search_param, limit, offset))
                establecimientos = cursor.fetchall()
        else:
            # Get all establecimientos with pagination
            sql = f"SELECT * FROM vw_establecimientos_list ORDER BY {order_by} {order_dir} LIMIT %s OFFSET %s"
            count_sql = "SELECT COUNT(*) as total FROM vw_establecimientos_list"
            
            with conn.cursor() as cursor:
                cursor.execute(count_sql)
                total = cursor.fetchone()['total']
                
                cursor.execute(sql, (limit, offset))
                establecimientos = cursor.fetchall()
        
        # Decrypt sensitive data for all establecimientos
        logger.info(f"Processing {len(establecimientos)} establecimientos")
        for establecimiento in establecimientos:
            estab_id = establecimiento['id']
            
            # Decrypt contact person name
            nombre_contacto_encrypted = establecimiento.get('nombre_contacto')
            apellido_paterno_encrypted = establecimiento.get('apellido_paterno_contacto')
            apellido_materno_encrypted = establecimiento.get('apellido_materno_contacto')
            
            nombre_contacto = decrypt_data(nombre_contacto_encrypted) if nombre_contacto_encrypted else None
            apellido_paterno = decrypt_data(apellido_paterno_encrypted) if apellido_paterno_encrypted else None
            apellido_materno = decrypt_data(apellido_materno_encrypted) if apellido_materno_encrypted else None
            
            # Build full contact name
            nombre_completo_contacto = []
            if nombre_contacto:
                nombre_completo_contacto.append(nombre_contacto)
            if apellido_paterno:
                nombre_completo_contacto.append(apellido_paterno)
            if apellido_materno:
                nombre_completo_contacto.append(apellido_materno)
            
            establecimiento['nombre_contacto_completo'] = ' '.join(nombre_completo_contacto) if nombre_completo_contacto else None
            
            # Remove individual encrypted name fields from response
            del establecimiento['nombre_contacto']
            del establecimiento['apellido_paterno_contacto']
            del establecimiento['apellido_materno_contacto']
            
            # Decrypt contact email
            correo_contacto_encrypted = establecimiento.get('correo_contacto')
            correo_contacto = decrypt_data(correo_contacto_encrypted) if correo_contacto_encrypted else None
            
            # Decrypt contact phone
            telefono_contacto_encrypted = establecimiento.get('telefono_contacto')
            telefono_contacto = decrypt_data(telefono_contacto_encrypted) if telefono_contacto_encrypted else None
            
            # Combine emails: "public@email.com / private@email.com"
            correo_publico = establecimiento.get('correo_publico')
            emails = []
            if correo_publico:
                emails.append(correo_publico)
            if correo_contacto:
                emails.append(correo_contacto)
            establecimiento['correo'] = ' / '.join(emails) if emails else None
            
            # Combine phones: "5555551234 / 5512345678"
            telefono_publico = establecimiento.get('telefono_publico')
            phones = []
            if telefono_publico:
                phones.append(telefono_publico)
            if telefono_contacto:
                phones.append(telefono_contacto)
            establecimiento['telefono'] = ' / '.join(phones) if phones else None
            
            # Remove individual fields to keep response clean
            del establecimiento['correo_publico']
            del establecimiento['correo_contacto']
            del establecimiento['telefono_publico']
            del establecimiento['telefono_contacto']
            
            logger.info(f"Establecimiento ID {estab_id}: Processed successfully")
        
        # Calculate pagination info
        total_pages = (total + limit - 1) // limit
        
        logger.info(f"Retrieved {len(establecimientos)} establecimientos (page {page} of {total_pages})")
        
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'data': establecimientos,
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