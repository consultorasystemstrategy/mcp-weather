import boto3
import logging
import os
import re
import unicodedata
from dotenv import load_dotenv
from io import BytesIO
from PyPDF2 import PdfReader
from docx import Document

from s3_helper import S3Helper
from dynamodb_helper import DynamoDBHelper

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='logs/utils.log', filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

S3_RESOURCES_BUCKET = os.getenv("S3_RESOURCES_BUCKET")
S3_PATH = "SOFIA_FILE/PLANIFICACION/AV_Recursos"

DYNAMO_CHAT_HISTORY_TABLE = os.getenv("DYNAMO_CHAT_HISTORY_TABLE")
DYNAMO_LIBRARY_TABLE = os.getenv("DYNAMO_LIBRARY_TABLE")
DYNAMO_RESOURCES_TABLE = os.getenv("DYNAMO_RESOURCES_TABLE")
DYNAMO_RESOURCES_HASH_TABLE = os.getenv("DYNAMO_RESOURCES_HASH_TABLE")

boto3.setup_default_session(profile_name='dev-upeu-admin')

# Crear helper instances
s3_helper = S3Helper(bucket_name=S3_RESOURCES_BUCKET)
files_table_helper = DynamoDBHelper(
    table_name=DYNAMO_RESOURCES_TABLE,
    pk_name="resource_id"
)
hash_table_helper = DynamoDBHelper(
    table_name=DYNAMO_RESOURCES_HASH_TABLE,
    pk_name="file_hash"
)
library_table_helper = DynamoDBHelper(
    table_name=DYNAMO_LIBRARY_TABLE,
    pk_name="silabus_id"
)

# Limpieza de nombres de archivos y extracción de extensiones
def sanitize_filename(filename: str) -> str:
    """
    Limpia caracteres especiales y espacios en el nombre del archivo.
    
    :param filename: Nombre original del archivo
    :return: Nombre sanitizado
    """
    normalized_name = unicodedata.normalize('NFKD', filename.lower()).encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r"[., ]", "_", normalized_name)

def get_file_extension(file_path: str) -> str:
    """
    Obtiene la extensión del archivo.
        
    :param file_path: Ruta al archivo
    :return: Extensión del archivo
    """
    return os.path.splitext(file_path)[1].lower().replace(".", "")

# Extraer texto de archivos PDF y DOCX en memoria
def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Extrae texto de un archivo PDF en memoria (bytes).
    
    :param pdf_bytes: Contenido binario del archivo PDF
    :return: Texto extraído
    """
    try:
        text = ""
        reader = PdfReader(BytesIO(pdf_bytes))
        
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text += page.extract_text() + "\n"

        logger.info("Texto extraído exitosamente del PDF en memoria.")
        return text

    except Exception as e:
        logger.error(f"Error extrayendo texto del PDF: {e}")
        raise e
    
def extract_text_from_docx_bytes(docx_bytes: bytes) -> str:
    """
    Extrae texto de un archivo DOCX en memoria (ej. S3, stream, etc.).

    :param docx_bytes: Contenido binario del archivo DOCX
    :return: Texto extraído
    """
    try:
        doc = Document(BytesIO(docx_bytes))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        extracted_text = "\n".join(paragraphs)
        logger.info("Texto extraído exitosamente de DOCX en memoria.")
        return extracted_text
    except Exception as e:
        logger.error(f"Error extrayendo texto de DOCX desde bytes: {e}")
        raise e

async def get_text_from_file_by_title(title: str) -> str:
    """
    Extrae texto de un archivo en memoria (bytes) según su tipo.

    :param title: Título del archivo
    :return: Texto extraído
    """
    try:
        file_type = get_file_extension(title)
        object_key = f"{S3_PATH}/{sanitize_filename(title)}"

        response = s3_helper.get_object(object_key=object_key)
        file_bytes = response['Body'].read()

        if file_type == "pdf":
            return extract_text_from_pdf_bytes(file_bytes)
        elif file_type == "docx" or file_type == "doc":
            return extract_text_from_docx_bytes(file_bytes)
        else:
            logger.error(f"Tipo de archivo no soportado: {file_type}")
            return ""
    except Exception as e:
        logger.error(f"Error obteniendo texto del archivo {file_type}: {e}")
        return ""
    
async def get_titles_resources_by_silabus(silabus_id: str) -> list[str]:
    """
    Obtiene los títulos de los recursos asociados a un silabo específico.

    :param silabus_id: ID del silabo
    :return: Lista de títulos de recursos
    """
    try:
        library_item = library_table_helper.get_item(silabus_id)
        if library_item and "resources" in library_item:
            resources = library_item["resources"]
        else:
            logger.warning(f"No se encontraron recursos para el silabo {silabus_id}")
            return []
        
        resource_ids = [resource["resource_id"] for resource in resources]
        logger.info(f"Se encontraron {len(resource_ids)} resource_id(s) en el silabo {silabus_id}")

        titles = []
        for resource_id in resource_ids:
            title = get_title_from_resource_id(resource_id)
            if title:
                titles.append(title)

        return titles
    except Exception as e:
        logger.error(f"Error obteniendo títulos de recursos para el silabo {silabus_id}: {e}")
        return []
    
def get_title_from_resource_id(resource_id: str) -> str:
    """
    Obtiene el título de un recurso dado su ID.

    :param resource_id: ID del recurso
    :return: Título del recurso o None si no se encuentra
    """
    try:
        item = files_table_helper.get_item(resource_id)
        if item and "resource_title" in item:
            return item["resource_title"]
        else:
            logger.warning(f"No se encontró el recurso con ID {resource_id}")
            return None
    except Exception as e:
        logger.error(f"Error obteniendo título del recurso {resource_id}: {e}")
        return None