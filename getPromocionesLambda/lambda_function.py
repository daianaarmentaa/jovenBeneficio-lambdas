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

def decimal_date_handler(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

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
    """Main Lambda handler for listing promociones."""
    
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
        estado_filter = query_params.get('estado', '').strip().lower()
        page = int(query_params.get('page', 1))
        limit = int(query_params.get('limit', 10))
        order_by = query_params.get('orderBy', 'id')
        order_dir = query_params.get('orderDir', 'DESC').upper()
        
        # Validate order parameters
        valid_order_by = ['id', 'nombre_promocion', 'nombre_establecimiento', 'fecha_creacion', 'fecha_expiracion', 'estado']
        if order_by not in valid_order_by:
            order_by = 'id'
        if order_dir not in ['ASC', 'DESC']:
            order_dir = 'DESC'
        
        # Calculate offset for pagination
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        
        # Build WHERE clause for filters
        where_conditions = []
        query_values = []
        
        # Search filter
        if search:
            where_conditions.append("(nombre_promocion LIKE %s OR nombre_establecimiento LIKE %s)")
            search_param = f"%{search}%"
            query_values.extend([search_param, search_param])
        
        # Estado filter
        if estado_filter and estado_filter in ['activa', 'expirada', 'cancelada']:
            where_conditions.append("estado = %s")
            query_values.append(estado_filter)
        
        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build queries
        sql = f"""
            SELECT * FROM vw_promociones_list
            {where_clause}
            ORDER BY {order_by} {order_dir}
            LIMIT %s OFFSET %s
        """
        
        count_sql = f"""
            SELECT COUNT(*) as total FROM vw_promociones_list
            {where_clause}
        """
        
        with conn.cursor() as cursor:
            # Get total count
            if query_values:
                cursor.execute(count_sql, tuple(query_values))
            else:
                cursor.execute(count_sql)
            total = cursor.fetchone()['total']
            
            # Get paginated results
            query_values.extend([limit, offset])
            cursor.execute(sql, tuple(query_values))
            promociones = cursor.fetchall()
        
        # Process promociones - handle foto S3 paths
        logger.info(f"Processing {len(promociones)} promociones")
        for promocion in promociones:
            # Handle foto field - should be S3 path (string)
            foto = promocion.get('foto')
            if foto and isinstance(foto, str):
                promocion['foto'] = foto
            else:
                promocion['foto'] = 'fotos_promociones/default-promo.jpg'
        
        # Calculate pagination info
        total_pages = (total + limit - 1) // limit
        
        logger.info(f"Retrieved {len(promociones)} promociones (page {page} of {total_pages})")
        
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'data': promociones,
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