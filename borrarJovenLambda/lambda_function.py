import json
import pymysql
import os
import boto3
import logging
from botocore.exceptions import ClientError

# Logger configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
secrets_manager_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')

# Environment variables
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

# Database connection cache
db_connection = None

# CORS Headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'DELETE, OPTIONS',
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
    
    try:
        if db_connection and db_connection.open:
            db_connection.ping(reconnect=True)
            return db_connection
    except:
        db_connection = None
    
    try:
        creds = get_db_credentials()
        logger.info("Connecting to database...")
        
        db_name = creds.get('database', 'beneficioJoven')
        logger.info(f"Attempting to connect to database: {db_name}")
        
        db_connection = pymysql.connect(
            host=creds['host'],
            user=creds['username'],
            password=creds['password'],
            database=db_name,
            connect_timeout=10
        )
        logger.info(f"Database connection successful to: {db_name}")
        return db_connection
    except pymysql.MySQLError as e:
        logger.error(f"Database connection error: {e}")
        raise

def delete_photo_from_s3(s3_key):
    """Delete photo from S3 bucket."""
    try:
        # Don't delete default avatar
        if s3_key == 'fotos_jovenes/default-avatar.jpg':
            logger.info("Skipping deletion of default avatar")
            return True
        
        s3_client.delete_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        logger.info(f"Photo deleted from S3: {s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 deletion error: {e}")
        return False

def lambda_handler(event, context):
    """Main Lambda handler for deleting a youth record."""
    
    logger.info(f"Event received: {json.dumps(event)}")
    
    # Get HTTP method
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    logger.info(f"HTTP method: {http_method}")
    
    # Handle OPTIONS preflight
    if http_method == 'OPTIONS':
        logger.info("Responding to OPTIONS preflight request")
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'OK'})
        }
    
    try:
        # Get user ID from path parameters
        path_parameters = event.get('pathParameters', {})
        user_id = path_parameters.get('id')
        
        if not user_id:
            logger.warning("Missing user ID in path parameters")
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'ID de joven requerido'
                })
            }
        
        # Validate ID is numeric
        try:
            user_id = int(user_id)
        except ValueError:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'ID inválido'
                })
            }
        
        logger.info(f"Attempting to delete youth with ID: {user_id}")
        
        # Get database connection
        conn = get_db_connection()
        
        # First, check if the youth exists and get their photo
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT id_usuario, nombre, apellido_paterno, foto 
                FROM Joven 
                WHERE id_usuario = %s
            """, (user_id,))
            
            joven = cursor.fetchone()
            
            if not joven:
                logger.warning(f"Youth with ID {user_id} not found")
                return {
                    'statusCode': 404,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({
                        'message': 'Joven no encontrado'
                    })
                }
            
            logger.info(f"Found youth: {joven['nombre']} {joven['apellido_paterno']}")
            foto_s3_key = joven.get('foto')
        
        # Delete related records in cascade order
        # 1. Delete from TarjetaPromocion (if exists)
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM TarjetaPromocion 
                WHERE id_tarjeta IN (
                    SELECT id_tarjeta FROM Tarjeta WHERE id_usuario = %s
                )
            """, (user_id,))
            deleted_promo = cursor.rowcount
            logger.info(f"Deleted {deleted_promo} records from TarjetaPromocion")
        
        # 2. Delete from Solicitud (if exists)
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM Solicitud 
                WHERE id_tarjeta IN (
                    SELECT id_tarjeta FROM Tarjeta WHERE id_usuario = %s
                )
            """, (user_id,))
            deleted_solicitud = cursor.rowcount
            logger.info(f"Deleted {deleted_solicitud} records from Solicitud")
        
        # 3. Delete from Tarjeta
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM Tarjeta WHERE id_usuario = %s
            """, (user_id,))
            deleted_tarjeta = cursor.rowcount
            logger.info(f"Deleted {deleted_tarjeta} records from Tarjeta")
        
        # 4. Delete from favoritos (if exists)
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM favoritos WHERE id_usuario = %s
            """, (user_id,))
            deleted_favoritos = cursor.rowcount
            logger.info(f"Deleted {deleted_favoritos} records from favoritos")
        
        # 5. Finally, delete from Joven
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM Joven WHERE id_usuario = %s
            """, (user_id,))
            deleted_joven = cursor.rowcount
        
        # Commit all deletions
        conn.commit()
        logger.info(f"Successfully deleted youth with ID: {user_id}")
        
        # Delete photo from S3 (non-blocking)
        if foto_s3_key:
            delete_photo_from_s3(foto_s3_key)
        
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Joven eliminado con éxito',
                'id': user_id,
                'nombre': f"{joven['nombre']} {joven['apellido_paterno']}",
                'records_deleted': {
                    'tarjeta_promocion': deleted_promo,
                    'solicitud': deleted_solicitud,
                    'tarjeta': deleted_tarjeta,
                    'favoritos': deleted_favoritos,
                    'joven': deleted_joven
                }
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        conn.rollback() if conn else None
        
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error de base de datos al eliminar el joven',
                'error': str(e)
            })
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        conn.rollback() if conn else None
        
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error interno del servidor',
                'error': str(e)
            })
        }