import json
import pymysql
import os
import bcrypt
import boto3
import base64
import logging
import hashlib
import datetime
from botocore.exceptions import ClientError

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

def hash_for_duplicate_check(data):
    """Create a SHA-256 hash for duplicate checking."""
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

def upload_photo_to_s3(photo_base64, establecimiento_nombre):
    """Upload base64 photo to S3."""
    try:
        image_data = base64.b64decode(photo_base64)
        # Sanitize filename
        safe_name = "".join(c for c in establecimiento_nombre if c.isalnum() or c in (' ', '-', '_')).strip()
        file_key = f"fotos_establecimientos/{safe_name}_{int(datetime.datetime.utcnow().timestamp())}.jpg"
        
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
    """Main Lambda handler for establishment registration."""
    
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
        required_fields = [
            'nombreEstablecimiento',  # Business name (public)
            'nombreContacto',         # Contact person name
            'apellidoPaternoContacto',
            'correoContacto',         # Contact email (encrypted)
            'telefonoContacto',       # Contact phone (encrypted)
            'password',
            'idCategoria',
            'idAdmin',
            'direccion',              # Object with address fields
            'consentimientoAceptado'
        ]
        
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
        
        # Validate consent (required by Mexican data protection law)
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
        
        # Create hash for contact phone duplicate checking
        telefono_hash = hash_for_duplicate_check(body['telefonoContacto'])
        
        # CHECK FOR DUPLICATES BEFORE ENCRYPTING
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Check contact email (will be encrypted, but check before encryption)
            cursor.execute(
                "SELECT id_Establecimiento FROM Establecimiento WHERE correo_contacto = %s",
                (body['correoContacto'],)
            )
            if cursor.fetchone():
                logger.warning(f"Duplicate contact email attempt: {body['correoContacto']}")
                return {
                    'statusCode': 409,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'El correo de contacto ya está registrado'})
                }
            
            # Check contact phone hash
            cursor.execute(
                "SELECT id_Establecimiento FROM Establecimiento WHERE telefono_hash_contacto = %s",
                (telefono_hash,)
            )
            if cursor.fetchone():
                logger.warning(f"Duplicate contact phone detected")
                return {
                    'statusCode': 409,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'El teléfono de contacto ya está registrado'})
                }
            
            # Check if business name already exists (optional, depends on your business rules)
            cursor.execute(
                "SELECT id_Establecimiento FROM Establecimiento WHERE nombre = %s",
                (body['nombreEstablecimiento'],)
            )
            if cursor.fetchone():
                logger.warning(f"Duplicate establishment name: {body['nombreEstablecimiento']}")
                return {
                    'statusCode': 409,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'Ya existe un establecimiento con este nombre'})
                }
        
        # Encrypt sensitive contact data (personal information)
        nombre_contacto_encrypted = encrypt_data(body['nombreContacto'])
        apellido_paterno_encrypted = encrypt_data(body['apellidoPaternoContacto'])
        apellido_materno_encrypted = encrypt_data(body.get('apellidoMaternoContacto', '')) if body.get('apellidoMaternoContacto') else None
        correo_contacto_encrypted = encrypt_data(body['correoContacto'])
        telefono_contacto_encrypted = encrypt_data(body['telefonoContacto'])
        logger.info("Sensitive contact data encrypted")
        
        # Handle photo
        foto_s3_key = 'fotos_establecimientos/default-establishment.jpg'
        if body.get('foto'):
            uploaded_key = upload_photo_to_s3(body['foto'], body['nombreEstablecimiento'])
            if uploaded_key:
                foto_s3_key = uploaded_key
        
        # Insert Establecimiento into database
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO Establecimiento (
                    id_categoria, id_admin, nombre_contacto, apellido_paterno_contacto,
                    apellido_materno_contacto, correo_contacto, telefono_hash_contacto,
                    contrasena_hash, nombre, foto, calle, colonia, codigo_postal,
                    municipio, numero_ext, numero_int, correo, numero_de_telefono
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                body['idCategoria'],
                body['idAdmin'],
                nombre_contacto_encrypted,
                apellido_paterno_encrypted,
                apellido_materno_encrypted,
                correo_contacto_encrypted,
                telefono_hash,
                hashed_password,
                body['nombreEstablecimiento'],  # Business name (public)
                foto_s3_key,
                body['direccion'].get('calle'),
                body['direccion'].get('colonia'),
                body['direccion'].get('codigoPostal'),
                body['direccion'].get('municipio'),
                body['direccion'].get('numeroExterior'),
                body['direccion'].get('numeroInterior'),
                body.get('correoPublico', body['correoContacto']),  # Public email (optional)
                body.get('telefonoPublico')  # Public phone (optional)
            ))
        
        conn.commit()
        establecimiento_id = cursor.lastrowid
        logger.info(f"Establecimiento registered successfully with ID: {establecimiento_id}")
        
        return {
            'statusCode': 201,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Establecimiento registrado con éxito',
                'id': establecimiento_id,
                'nombre': body['nombreEstablecimiento']
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        
        # Check for duplicate entry (backup check)
        if e.args[0] == 1062:
            return {
                'statusCode': 409,
                'headers': CORS_HEADERS,
                'body': json.dumps({'message': 'El correo o teléfono de contacto ya están registrados'})
            }
        
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'Error de base de datos'})
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