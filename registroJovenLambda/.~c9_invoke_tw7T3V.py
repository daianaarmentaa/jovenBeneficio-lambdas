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

def upload_photo_to_s3(photo_base64, curp):
    """Upload base64 photo to S3."""
    try:
        image_data = base64.b64decode(photo_base64)
        file_key = f"fotos_jovenes/{curp}_{int(datetime.datetime.utcnow().timestamp())}.jpg"
        
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

def calculate_luhn(s):
    """Calculate Luhn check digit for validation."""
    digits = [int(d) for d in s if d.isdigit()]
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        n = d * 2 if i % 2 == 0 else d
        checksum += n if n < 10 else n - 9
    return (10 - (checksum % 10)) % 10

def generate_new_folio(user_id):
    """
    Generate new digital folio: BJ-YYYY-MM-NNNNNN-C
    Uses user_id as sequential number.
    """
    now = datetime.datetime.utcnow()
    year = now.year
    month = str(now.month).zfill(2)
    
    # Use database ID as sequential number
    seq = str(user_id).zfill(6)
    
    # Create base for check digit
    base = f"BJ{year}{month}{seq}"
    check_digit = calculate_luhn(base)
    
    # Format with dashes
    folio = f"BJ-{year}-{month}-{seq}-{check_digit}"
    
    return folio

def get_next_legacy_folio(conn):
    """
    Get next sequential legacy folio: 1234567890120XXX
    First 12 digits fixed, last 4 sequential.
    """
    with conn.cursor() as cursor:
        # Get the highest legacy folio
        cursor.execute("""
            SELECT folio_legacy 
            FROM Tarjeta 
            WHERE folio_legacy IS NOT NULL 
            AND folio_legacy LIKE '123456789012%'
            ORDER BY folio_legacy DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            last_folio = result[0]
            # Extract last 4 digits and increment
            last_number = int(last_folio[-4:])
            next_number = last_number + 1
            
            if next_number > 9999:
                raise ValueError("Legacy folio sequence exhausted (max 9999)")
            
            next_folio = f"123456789012{str(next_number).zfill(4)}"
        else:
            # First legacy folio
            next_folio = "1234567890120001"
        
        logger.info(f"Generated next legacy folio: {next_folio}")
        return next_folio

def validate_legacy_folio(folio):
    """
    Validate old system folio format.
    Must be exactly 16 digits, start with 123456789012, last 4 between 0001-9999.
    """
    # Remove spaces and dashes
    folio = folio.replace(' ', '').replace('-', '')
    
    # Must be 16 digits
    if len(folio) != 16:
        return False, "Folio debe tener 16 dígitos"
    
    # Must be all digits
    if not folio.isdigit():
        return False, "Folio debe contener solo números"
    
    # Check if starts with expected prefix
    if not folio.startswith('123456789012'):
        return False, "Formato de folio inválido"
    
    # Check last 4 digits are reasonable (0001-9999)
    last_four = int(folio[-4:])
    if last_four < 1 or last_four > 9999:
        return False, "Número de folio fuera de rango válido"
    
    return True, "Válido"

def lambda_handler(event, context):
    """Main Lambda handler for youth registration with dual folio system."""
    
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
        
        # Create hash for CURP duplicate checking
        curp_hash = hash_for_duplicate_check(body['curp'])
        
        # Get optional legacy folio
        folio_antiguo = body.get('folio_antiguo')
        folio_legacy = None
        tipo_tarjeta = 'digital'
        
        # CHECK FOR DUPLICATES BEFORE ENCRYPTING
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Check email
            cursor.execute("SELECT id_usuario FROM Joven WHERE correo = %s", (body['correo'],))
            if cursor.fetchone():
                logger.warning(f"Duplicate email attempt: {body['correo']}")
                return {
                    'statusCode': 409,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'El correo ya está registrado'})
                }
            
            # Check CURP hash
            cursor.execute("SELECT id_usuario FROM Joven WHERE curp_hash = %s", (curp_hash,))
            if cursor.fetchone():
                logger.warning(f"Duplicate CURP detected")
                return {
                    'statusCode': 409,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'El CURP ya está registrado'})
                }
        
        # Handle legacy folio
        if folio_antiguo:
            # User provided old folio - validate it
            is_valid, message = validate_legacy_folio(folio_antiguo)
            if not is_valid:
                return {
                    'statusCode': 400,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': f'Folio antiguo inválido: {message}'})
                }
            
            # Clean format
            folio_legacy = folio_antiguo.replace(' ', '').replace('-', '')
            
            # Check if already used
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id_usuario FROM Tarjeta WHERE folio_legacy = %s",
                    (folio_legacy,)
                )
                if cursor.fetchone():
                    return {
                        'statusCode': 409,
                        'headers': CORS_HEADERS,
                        'body': json.dumps({'message': 'Este folio antiguo ya está registrado'})
                    }
            
            tipo_tarjeta = 'mixta'
            logger.info(f"User provided legacy folio: {folio_legacy}, tipo: mixta")
        else:
            # Generate next sequential legacy folio
            try:
                folio_legacy = get_next_legacy_folio(conn)
                tipo_tarjeta = 'digital'
                logger.info(f"Auto-generated legacy folio: {folio_legacy}, tipo: digital")
            except ValueError as e:
                logger.error(f"Legacy folio generation failed: {e}")
                return {
                    'statusCode': 500,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'message': 'Error generando folio legacy'})
                }
        
        # Encrypt sensitive data
        curp_encrypted = encrypt_data(body['curp'])
        telefono_encrypted = encrypt_data(body['celular']) if body.get('celular') else None
        logger.info("Sensitive data encrypted")
        
        # Handle photo
        foto_s3_key = 'default-avatar.JPG'
        if body.get('foto'):
            uploaded_key = upload_photo_to_s3(body['foto'], body['curp'])
            if uploaded_key:
                foto_s3_key = uploaded_key
        
        # Insert Joven into database
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO Joven (
                    nombre, apellido_paterno, apellido_materno, curp, curp_hash, fecha_nacimiento, 
                    telefono, foto, genero, contrasena, correo, calle, colonia, 
                    codigo_postal, municipio, numero_ext, numero_int, consentimiento_aceptado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(sql, (
                body.get('nombre'),
                body.get('apellidoPaterno'),
                body.get('apellidoMaterno'),
                curp_encrypted,
                curp_hash,
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
        user_id = cursor.lastrowid
        logger.info(f"Youth registered successfully with ID: {user_id}")
        
        # Generate new digital folio
        folio_digital = generate_new_folio(user_id)
        logger.info(f"Generated digital folio: {folio_digital}")
        
        # Create Tarjeta record with both folios
        # NOTE: fecha_obtencion and fecha_expiracion are set by the database trigger
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO Tarjeta (
                    id_usuario, folio, folio_legacy, tipo, estado
                ) VALUES (%s, %s, %s, %s, 'activa')
            """, (user_id, folio_digital, folio_legacy, tipo_tarjeta))
            tarjeta_id = cursor.lastrowid
            logger.info(f"Tarjeta created with ID: {tarjeta_id}")
        
        conn.commit()
        
        return {
            'statusCode': 201,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Joven registrado con éxito',
                'id': user_id,
                'folio_digital': folio_digital,
                'folio_legacy': folio_legacy,
                'tipo': tipo_tarjeta
            })
        }
    
    except pymysql.MySQLError as e:
        logger.error(f"Database error: {e}")
        
        # Check for duplicate entry (backup check)
        if e.args[0] == 1062:
            return {
                'statusCode': 409,
                'headers': CORS_HEADERS,
                'body': json.dumps({'message': 'El correo o CURP ya están registrados'})
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