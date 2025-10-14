import json
import pymysql
import os
import bcrypt
import boto3
import base64
import logging
from botocore.exceptions import ClientError
from datetime import datetime

# Logger configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
secrets_manager_client = boto3.client('secretsmanager')
kms_client = boto3.client('kms')
s3_client = boto3.client('s3')

# Environment variables
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME')
KMS_KEY_ID = os.environ.get('KMS_KEY_ID')
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
    if db_connection and db_connection.open:
        return db_connection
    
    try:
        creds = get_db_credentials()
        logger.info("Connecting to database...")
        db_connection = pymysql.connect(
            host=creds['host'],
            user=creds['username'],
            password=creds['password'],
            database=creds['dbname'],
            connect_timeout=5
        )
        logger.info("Database connection successful")
        return db_connection
    except pymysql.MySQLError as e:
        logger.error(f"Database connection error: {e}")
        raise

def encrypt_data(data):
    """Encrypt data using KMS."""
    try:
        response = kms_client.encrypt(
            KeyId=KMS_KEY_ID,
            Plaintext=bytes(data, 'utf-8')
        )
        return base64.b64encode(response['CiphertextBlob']).decode('utf-8')
    except ClientError as e:
        logger.error(f"KMS encryption error: {e}")
        raise

def upload_photo_to_s3(photo_base64, curp):
    """Upload base64 photo to S3."""
    try:
        image_data = base64.b64decode(photo_base64)
        file_key = f"fotos_jovenes/{curp}_{int(datetime.utcnow().timestamp())}.jpg"
        
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
    """Main Lambda handler for youth registration."""
    
    logger.info(f"Event received: {json.dumps(event)}")
    
    # Get HTTP method (compatible with HTTP API and REST API)
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    logger.info(f"HTTP method: {http_method}")
    
    # Handle OPTIONS preflight request
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
        required_fields = ['nombre', 'apellidoPaterno', 'curp', 'correo', 'password', 'consentimientoAceptado']
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
        
        # Validate consent
        if not body['consentimientoAceptado']:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'El consentimiento del aviso de privacidad es obligatorio'
                })
            }
        
        # Hash password
        password = body['password'].encode('utf-8')
        hashed_password = bcrypt.hashpw(password, bcrypt.gensalt()).decode('utf-8')
        logger.info("Password hashed successfully")
        
        # Encrypt sensitive data
        curp_encrypted = encrypt_data(body['curp'])
        telefono_encrypted = encrypt_data(body['celular']) if body.get('celular') else None
        logger.info("Sensitive data encrypted")
        
        # Upload photo if provided
        foto_s3_key = None
        if body.get('foto'):
            foto_s3_key = upload_photo_to_s3(body['foto'], body['curp'])
        
        # Insert into database
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO Joven (
                    nombre, apellido_paterno, apellido_materno, curp, fecha_nacimiento, 
                    telefono, foto, genero, contrasena, correo, calle, colonia, 
                    codigo_postal, municipio, numero_ext, numero_int, consentimiento_aceptado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(sql, (
                body.get('nombre'),
                body.get('apellidoPaterno'),
                body.get('apellidoMaterno'),
                curp_encrypted,
                body.get('fechaNacimiento'),
                telefono_encrypted,
                foto_s3_key,
                body.get('genero'),
                hashed_password,
                body.get('correo'),
                body.get('direccion', {}).get('calle'),
                body.get('direccion', {}).get('colonia'),
                body.get('direccion', {}).get('codigoPostal'),
                body.get('direccion', {}).get('municipio'),
                body.get('direccion', {}).get('numeroExterior'),
                body.get('direccion', {}).get('numeroInterior')
            ))
        
        conn.commit()
        logger.info("Youth registered successfully in database")
        
        return {
            'statusCode': 201,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Joven registrado con éxito',
                'id': cursor.lastrowid
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        
        # Check for duplicate entry
        if e.args[0] == 1062:
            return {
                'statusCode': 409,
                'headers': CORS_HEADERS,
                'body': json.dumps({
                    'message': 'El correo o CURP ya están registrados'
                })
            }
        
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error de base de datos'
            })
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Error interno del servidor',
                'error': str(e)  # Remove in production
            })
        }