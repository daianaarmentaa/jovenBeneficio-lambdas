import json
import pymysql
import os
import boto3
import base64
import logging
import datetime
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
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
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

def upload_photo_to_s3(photo_base64, promocion_nombre):
    """Upload base64 photo to S3."""
    try:
        # Remove data:image prefix if present
        if ',' in photo_base64:
            photo_base64 = photo_base64.split(',')[1]
        
        image_data = base64.b64decode(photo_base64)
        
        # Sanitize filename
        safe_name = "".join(c for c in promocion_nombre if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_name = safe_name.replace(' ', '_')
        file_key = f"fotos_promociones/{safe_name}_{int(datetime.datetime.utcnow().timestamp())}.jpg"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=image_data,
            ContentType='image/jpeg'
        )
        logger.info(f"Photo uploaded to S3: {file_key}")
        return file_key
    except Exception as e:
        logger.error(f"S3 upload error: {e}")
        return None

def lambda_handler(event, context):
    """Main Lambda handler for creating a promotion."""
    
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
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        logger.info("Request body parsed successfully")
        
        # Validate required fields
        required_fields = ['id_establecimiento', 'nombre', 'descripcion', 'fecha_expiracion']
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'Faltan campos requeridos',
                    'missing_fields': missing_fields
                })
            }
        
        # Extract fields
        id_establecimiento = body['id_establecimiento']
        nombre = body['nombre']
        descripcion = body['descripcion']
        fecha_expiracion = body['fecha_expiracion']
        foto_base64 = body.get('foto')  # Optional
        
        # Validate establecimiento exists
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id_Establecimiento, nombre 
                FROM Establecimiento 
                WHERE id_Establecimiento = %s
            """, (id_establecimiento,))
            
            establecimiento = cursor.fetchone()
            
            if not establecimiento:
                logger.warning(f"Establishment with ID {id_establecimiento} not found")
                return {
                    'statusCode': 404,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({
                        'message': 'Establecimiento no encontrado'
                    })
                }
        
        # Validate date format and that it's in the future
        try:
            expiration_date = datetime.datetime.strptime(fecha_expiracion, '%Y-%m-%d').date()
            today = datetime.date.today()
            
            if expiration_date <= today:
                return {
                    'statusCode': 400,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({
                        'message': 'La fecha de expiración debe ser futura'
                    })
                }
        except ValueError:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'Formato de fecha inválido. Use YYYY-MM-DD'
                })
            }
        
        # Upload photo if provided
        foto_s3_key = 'fotos_promociones/default-promotion.jpg'
        if foto_base64:
            uploaded_key = upload_photo_to_s3(foto_base64, nombre)
            if uploaded_key:
                foto_s3_key = uploaded_key
        
        # Insert promotion into database
        # Note: fecha_creacion will be set by database (MUL constraint suggests it has a default)
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO Promocion (
                    id_establecimiento, 
                    nombre, 
                    descripcion, 
                    fecha_creacion,
                    fecha_expiracion,
                    foto,
                    estado
                ) VALUES (%s, %s, %s, CURDATE(), %s, %s, 'activa')
            """
            
            cursor.execute(sql, (
                id_establecimiento,
                nombre,
                descripcion,
                fecha_expiracion,
                foto_s3_key
            ))
        
        conn.commit()
        promocion_id = cursor.lastrowid
        logger.info(f"Promotion created successfully with ID: {promocion_id}")
        
        return {
            'statusCode': 201,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Promoción creada con éxito',
                'id': promocion_id,
                'nombre': nombre,
                'id_establecimiento': id_establecimiento,
                'foto': foto_s3_key,
                'fecha_expiracion': fecha_expiracion,
                'estado': 'activa'
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        conn.rollback() if conn else None
        
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error de base de datos',
                'error': str(e)
            })
        }
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return {
            'statusCode': 400,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'JSON inválido en el cuerpo de la petición'
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