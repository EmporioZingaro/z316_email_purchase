import json
import logging
import time
from typing import Dict

import requests
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud.exceptions import BadRequest
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Asm, Mail
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential, before_sleep_log

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

PROJECT_ID = "emporio-zingaro"
SECRET_MANAGER_API_TOKEN_NAME = "projects/559935551835/secrets/z316-tiny-token-api/versions/latest"
SECRET_MANAGER_SENDGRID_API_KEY_NAME = "projects/559935551835/secrets/SendGrid/versions/latest"
TEMPLATE_ID = 'd-f5543523eceb42bc9eec353aebc19aef'
FROM_EMAIL = 'sac@emporiozingaro.com'
TEST_MODE = True
TEST_EMAIL = 'rodrigo@brunale.com'

storage_client = storage.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

TINY_ERP_API_TOKEN = ""
SENDGRID_API_TOKEN = ""


def get_api_token(secret_name: str) -> str:
    """Retrieves a secret value from Google Cloud Secret Manager."""
    try:
        response = secret_manager_client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to retrieve API token {secret_name}: {e}")
        raise


def initialize_globals():
    """Initialize global variables that require API calls."""
    global TINY_ERP_API_TOKEN, SENDGRID_API_TOKEN
    TINY_ERP_API_TOKEN = get_api_token(SECRET_MANAGER_API_TOKEN_NAME)
    SENDGRID_API_TOKEN = get_api_token(SECRET_MANAGER_SENDGRID_API_KEY_NAME)


class ValidationError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class RetryableError(Exception):
    pass


def trigger_function(event, context):
    initialize_globals()

    file_name = event['name']
    bucket_name = event['bucket']
    logging.info(f"Processing file: {file_name} from bucket: {bucket_name}")
    
    sg_client = SendGridAPIClient(SENDGRID_API_TOKEN)

    file_data = download_blob(bucket_name, file_name)
    if file_data:
        process_webhook_payload(json.loads(file_data), sg_client)


def download_blob(bucket_name: str, source_blob_name: str) -> str:
    """Downloads a blob from the bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        return blob.download_as_string()
    except Exception as e:
        logging.error(f"Failed to download blob {source_blob_name} from bucket {bucket_name}: {e}")
        raise


def process_webhook_payload(payload: Dict, sg_client):
    try:
        if 'dados' not in payload:
            logging.error("Payload missing 'dados' key.")
            return

        dados_id = payload['dados'].get('id')
        if not dados_id:
            logging.error("Missing 'dados.id' in payload.")
            return

        try:
            nfce_id = generate_nfce(dados_id)
        except Exception as e:
            logging.error(f"Error during NFCe generation: {e}")
            nfce_id = None

        cliente_info = payload['dados'].get('cliente', {})
        cliente_nome = cliente_info.get('nome', 'Unknown Client')
        cliente_cpfCnpj = cliente_info.get('cpfCnpj')

        logging.info(f"Cliente Nome: {cliente_nome}, CPF/CNPJ: {cliente_cpfCnpj}")

        if cliente_cpfCnpj is None:
            logging.warning(f"NFCe was emitted for {cliente_nome}, but CPF/CNPJ is missing. No email will be sent.")
            return

        client_email = get_client_email(cliente_cpfCnpj)
        if not client_email:
            logging.warning(f"No email found for {cliente_nome} with CPF/CNPJ: {cliente_cpfCnpj}. NFCe was emitted, but no email will be sent.")
            return

        nota_fiscal_url = None

        if nfce_id:
            try:
                nota_fiscal_url = get_nota_fiscal_link(nfce_id)
            except Exception as e:
                logging.error(f"Error fetching NFCe link: {e}")

        email_data = aggregate_email_data(cliente_cpfCnpj, dados_id, client_email, nota_fiscal_url, cliente_nome)
        
        send_email(email_data, sg_client)

    except KeyError as e:
        logging.error(f"Key error in payload processing: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in payload processing: {e}")


@retry(wait=wait_exponential(multiplier=2.5, min=30, max=187.5), stop=stop_after_attempt(4), retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)))
def make_api_call(url: str) -> Dict:
    """Makes an API call with retry logic, URL sanitization, and enhanced error handling, using logging for error messages."""
    try:
        sanitized_url = url.split('?token=')[0]
        logging.info(f"Making API call to: {sanitized_url}")

        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        validate_json_payload(json_data)

        return json_data
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise
    except ValidationError as e:
        logging.error(f"Payload validation failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


def validate_json_payload(json_data: Dict):
    """Validates the JSON payload with enhanced error details."""
    status_processamento = json_data.get('retorno', {}).get('status_processamento')
    if status_processamento == '3':
        return
    elif status_processamento == '2':
        raise ValidationError("Invalid query parameter.")
    elif status_processamento == '1':
        codigo_erro = json_data.get('retorno', {}).get('codigo_erro', '')
        erros = json_data.get('retorno', {}).get('erros', [])
        erro_message = erros[0]['erro'] if erros else "Unknown error"
        if codigo_erro == '1':
            raise InvalidTokenError(f"Token is not valid: {erro_message}")
        else:
            raise RetryableError(f"Error encountered, will attempt retry: {erro_message}")


def generate_nfce(dados_id: str) -> str:
    """Generates NFCe for the given dados_id and returns idNotafiscal."""
    logging.info(f"Starting NFC-e generation for dados_id: {dados_id}")
    token = TINY_ERP_API_TOKEN
    url = f"https://api.tiny.com.br/api2/gerar.nota.fiscal.pedido.php?token={token}&formato=JSON&id={dados_id}&modelo=NFCe"
    
    response = make_api_call(url)
    
    if 'retorno' in response and 'registros' in response['retorno'] and 'registro' in response['retorno']['registros']:
        nfce_id = response['retorno']['registros']['registro']['idNotaFiscal']
        logging.info(f"NFC-e generated successfully with idNotaFiscal: {nfce_id}")
        return nfce_id
    else:
        raise ValidationError("NFCe generation response is missing expected fields.")


def get_nota_fiscal_link(idNotafiscal: str) -> str:
    logging.info(f"Fetching Nota Fiscal link for idNotafiscal: {idNotafiscal}")
    token = TINY_ERP_API_TOKEN
    url = f"https://api.tiny.com.br/api2/nota.fiscal.obter.link.php?token={token}&formato=JSON&id={idNotafiscal}"

    response = make_api_call(url)
    logging.debug(f"API call made to fetch Nota Fiscal link for idNotafiscal: {idNotafiscal}")

    if 'retorno' in response and 'link_nfe' in response['retorno']:
        nota_fiscal_link = response['retorno']['link_nfe']
        logging.info(f"Successfully fetched Nota Fiscal link for idNotafiscal: {idNotafiscal}, Link: {nota_fiscal_link}")
        return nota_fiscal_link
    else:
        logging.error("Nota Fiscal link response is missing expected fields.")
        raise ValidationError("Nota Fiscal link response is missing expected fields.")


def get_client_email_from_tinyerp(cliente_cpfCnpj: str) -> str:
    logging.info(f"Fetching email for client with CPF/CNPJ from TinyERP: {cliente_cpfCnpj}")
    token = TINY_ERP_API_TOKEN
    url = f"https://api.tiny.com.br/api2/contatos.pesquisa.php?token={token}&formato=JSON&cpf_cnpj={cliente_cpfCnpj}"

    response = make_api_call(url)
    logging.debug(f"API call made to fetch client email from TinyERP for CPF/CNPJ: {cliente_cpfCnpj}")

    contatos = response.get('retorno', {}).get('contatos', [])
    if contatos:
        contato = contatos[0].get('contato', {})
        email = contato.get('email')
        if email:
            logging.info(f"Successfully fetched email from TinyERP for CPF/CNPJ: {cliente_cpfCnpj}, Email: {email}")
            return email
        else:
            logging.warning(f"No email found in TinyERP for CPF/CNPJ: {cliente_cpfCnpj}")
    else:
        logging.warning(f"No contact information found in TinyERP for CPF/CNPJ: {cliente_cpfCnpj}")

    return None


def get_client_email(cliente_cpfCnpj: str) -> str:
    logging.info(f"Fetching email for client with CPF/CNPJ: {cliente_cpfCnpj}")
    try:
        query = f"""
        SELECT email 
        FROM `emporio-zingaro.z316_tiny.z316-tiny-contatos` 
        WHERE cpf_cnpj = '{cliente_cpfCnpj}'
        """
        logging.debug("Constructed SQL query for client email.")
        logging.debug(f"SQL Query: {query}")

        logging.debug("Executing BigQuery query for client email.")
        query_job = bq_client.query(query)
        logging.debug("Waiting for query job to complete...")
        results = query_job.result()
        logging.debug("Query job completed.")

        email_found = False
        for row in results:
            if row.email:
                email_found = True
                logging.info(f"Email found: {row.email}")
                return row.email
        
        if not email_found:
            logging.warning(f"No email found in BigQuery for client with CPF/CNPJ: {cliente_cpfCnpj}. Falling back to TinyERP.")
            return get_client_email_from_tinyerp(cliente_cpfCnpj)

    except BadRequest as e:
        logging.error(f"BigQuery BadRequest Error while fetching email for client {cliente_cpfCnpj}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error fetching email for client {cliente_cpfCnpj}: {e}")
        raise


@retry(wait=wait_exponential(multiplier=30, min=30, max=90),
       stop=stop_after_attempt(4),
       before_sleep=before_sleep_log(logging, logging.INFO))
def get_purchase_details(dados_id: str) -> dict:
    logging.info(f"Fetching purchase details for dados_id: {dados_id}")

    query = f"""
    SELECT
      item.id AS item_id,
      item.descricao AS item_name,
      item.quantidade AS item_quantity,
      item.valor AS item_price,
      (item.quantidade * item.valor) AS total_item_price,
      sub.desconto AS total_discount,
      sub.totalVenda AS total_paid,
      sub.formaPagamento AS payment_method,
      SUM(item.quantidade * item.valor) OVER() AS sub_total
    FROM (
      SELECT
        id,
        desconto,
        totalVenda,
        formaPagamento,
        itens
      FROM
        `emporio-zingaro.z316_tiny_raw_json.pdv`
      WHERE
        id = {dados_id}
    ) AS sub
    CROSS JOIN
      UNNEST(sub.itens) AS item
    """

    try:
        logging.debug("Constructed SQL query for purchase details.")
        logging.debug(f"SQL Query: {query}")

        logging.debug(f"Executing BigQuery query for purchase details.")
        query_job = bq_client.query(query)
        logging.debug("Waiting for query job to complete...")
        results = query_job.result()
        logging.debug("Query job completed.")

        items = []
        purchase_summary = {}
        for row in results:
            item_detail = {
                'item_name': row.item_name,
                'item_quantity': row.item_quantity,
                'item_price': row.item_price,
                'total_item_price': row.total_item_price
            }
            items.append(item_detail)

            purchase_summary.update({
                'total_discount': row.total_discount,
                'total_paid': row.total_paid,
                'payment_method': row.payment_method,
                'sub_total': row.sub_total
            })

        if not items:
            logging.warning(f"No purchase details found for ID: {dados_id}. Data might be delayed or ID is incorrect.")
            raise Exception("Purchase details not found after retries.")

        logging.info(f"Items details: {items}")
        logging.info(f"Purchase summary for dados_id {dados_id}: {purchase_summary}")

        purchase_summary['items'] = items
        return purchase_summary

    except BadRequest as e:
        logging.error(f"BigQuery BadRequest Error while fetching purchase details for dados_id {dados_id}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while fetching purchase details for dados_id {dados_id}: {e}")
        raise


def get_daily_checkins(cliente_cpfCnpj: str) -> dict:
    logging.info(f"Fetching daily check-ins for client with CPF/CNPJ: {cliente_cpfCnpj}")
    try:
        query = f"""
        SELECT
          COUNT(DISTINCT FORMAT_DATE('%Y-%m-%d', data)) AS daily_checkins
        FROM
          `emporio-zingaro.z316_tiny_raw_json.pdv`
        WHERE
          contato.cpfCnpj = '{cliente_cpfCnpj}'
          AND EXTRACT(QUARTER FROM data) = EXTRACT(QUARTER FROM CURRENT_DATE())
          AND EXTRACT(YEAR FROM data) = EXTRACT(YEAR FROM CURRENT_DATE());
        """
        logging.debug("Constructed SQL query for daily check-ins.")
        logging.debug(f"SQL Query: {query}")

        logging.debug("Executing BigQuery query for daily check-ins.")
        query_job = bq_client.query(query)
        logging.debug("Waiting for query job to complete...")
        results = query_job.result()
        logging.debug("Query job completed.")

        row = next(iter(results), None)
        daily_checkins = row.daily_checkins if row else 0

        logging.info(f"Daily check-ins fetched: {daily_checkins} for client {cliente_cpfCnpj}")
    except BadRequest as e:
        logging.error(f"BigQuery BadRequest Error while fetching daily check-ins for client {cliente_cpfCnpj}: {e}")
        daily_checkins = 0
    except Exception as e:
        logging.error(f"Error fetching daily check-ins for client {cliente_cpfCnpj}: {e}")
        daily_checkins = 0

    return {'daily_checkins': daily_checkins}


def get_quarter_spend(cliente_cpfCnpj: str) -> dict:
    logging.info(f"Fetching quarter spend for client with CPF/CNPJ: {cliente_cpfCnpj}")
    try:
        query = f"""
        SELECT
          SUM(sub.totalVenda) AS quarter_spend
        FROM (
          SELECT
            pdv.totalVenda,
            ARRAY_LENGTH(ARRAY(
              SELECT AS STRUCT item
              FROM UNNEST(pdv.itens) item
              WHERE item.desconto = '0.00'
            )) AS no_discount_items_count,
            ARRAY_LENGTH(pdv.itens) AS total_items_count
          FROM
            `emporio-zingaro.z316_tiny_raw_json.pdv` AS pdv
          WHERE
            pdv.contato.cpfCnpj = '{cliente_cpfCnpj}'
            AND EXTRACT(QUARTER FROM pdv.data) = EXTRACT(QUARTER FROM CURRENT_DATE())
            AND EXTRACT(YEAR FROM pdv.data) = EXTRACT(YEAR FROM CURRENT_DATE())
            AND pdv.formaPagamento IN ('credito', 'debito', 'pix', 'multiplas', 'dinheiro')
            AND pdv.desconto IN ('0', '0,00')
        ) AS sub
        WHERE
          sub.no_discount_items_count = sub.total_items_count;
        """
        logging.debug("Constructed SQL query for quarter spend.")
        logging.debug(f"SQL Query: {query}")

        logging.debug("Executing BigQuery query for quarter spend.")
        query_job = bq_client.query(query)
        logging.debug("Waiting for query job to complete...")
        results = query_job.result()
        logging.debug("Query job completed.")

        quarter_spend = next((row.quarter_spend for row in results), 0)
        logging.info(f"Quarter spend fetched: {quarter_spend} for client {cliente_cpfCnpj}")

    except BadRequest as e:
        logging.error(f"BigQuery BadRequest Error while fetching quarter spend for client {cliente_cpfCnpj}: {e}")
        quarter_spend = 0
    except Exception as e:
        logging.error(f"Error fetching quarter spend for client {cliente_cpfCnpj}: {e}")
        quarter_spend = 0

    return {'quarter_spend': quarter_spend}


def get_lifetime_spend(cliente_cpfCnpj: str) -> dict:
    logging.info(f"Fetching lifetime spend for client with CPF/CNPJ: {cliente_cpfCnpj}")

    query = f"""
	SELECT
	  SUM(sub.totalVenda) AS total_spend
	FROM (
	  SELECT
	    pdv.totalVenda,
	    ARRAY_LENGTH(ARRAY(
	      SELECT AS STRUCT item
	      FROM UNNEST(pdv.itens) item
	      WHERE item.desconto = '0.00'
	    )) AS no_discount_items_count,
	    ARRAY_LENGTH(pdv.itens) AS total_items_count
	  FROM
	    `emporio-zingaro.z316_tiny_raw_json.pdv` AS pdv
	  WHERE
	    pdv.contato.cpfCnpj = '{cliente_cpfCnpj}'
	    AND pdv.data >= '2023-10-01'
	    AND pdv.formaPagamento IN ('credito', 'debito', 'pix', 'multiplas', 'dinheiro')
	    AND pdv.desconto IN ('0', '0,00')
	) AS sub
	WHERE
	  sub.no_discount_items_count = sub.total_items_count;
    """

    logging.debug("Constructed SQL query for lifetime spend.")
    logging.debug(f"SQL Query: {query}")

    try:
        logging.debug("Executing BigQuery query for lifetime spend.")
        query_job = bq_client.query(query)

        # Wait for the query to finish
        logging.debug("Waiting for query job to complete...")
        query_job.result()
        logging.debug("Query job completed.")

        if query_job.state == "DONE" and not query_job.errors:
            logging.info("Query executed successfully.")
        else:
            logging.error(f"Query job didn't finish successfully. State: {query_job.state}, Errors: {query_job.errors}")

        results = query_job.result()
        rows = list(results)

        logging.debug(f"Number of rows returned by the query: {len(rows)}")

        if rows:
            total_spend = next((row.total_spend for row in rows if row.total_spend is not None), 0)
            logging.info(f"Total lifetime spend calculated: {total_spend}")
        else:
            total_spend = 0
            logging.warning("Query returned no rows. Setting total_spend to 0.")

    except BadRequest as e:
        logging.error(f"BigQuery BadRequest Error while fetching lifetime spend for client {cliente_cpfCnpj}: {e}")
        total_spend = 0
    except Exception as e:
        logging.error(f"Unexpected error while fetching lifetime spend for client {cliente_cpfCnpj}: {e}")
        total_spend = 0

    logging.debug(f"Returning total_spend: {total_spend} for client {cliente_cpfCnpj}")
    return {'total_spend': total_spend}


def aggregate_email_data(cliente_cpfCnpj: str, dados_id: str, client_email: str, nota_fiscal_url: str, client_name: str) -> dict:
    try:
        items = get_purchase_details(dados_id)
        daily_checkins = get_daily_checkins(cliente_cpfCnpj)
        quarter_spend = get_quarter_spend(cliente_cpfCnpj)
        lifetime_spend = get_lifetime_spend(cliente_cpfCnpj)
                
        email_data = {
	        'client_email': client_email,
	        'client_name': client_name,
	        'dados_id': dados_id,
	        'items': items.get('items', []),
	        'sub_total': items.get('sub_total', '0.00'),
	        'total_discount': items.get('total_discount', '0.00'),
	        'total_paid': items.get('total_paid', '0.00'),
	        'payment_method': items.get('payment_method', 'N/A'),
	        'daily_checkins': daily_checkins.get('daily_checkins', 0),
	        'quarter_spend': quarter_spend.get('quarter_spend', '0.00'),
	        'lifetime_spend': lifetime_spend.get('total_spend', '0.00'),
        }

        if nota_fiscal_url is not None:
            email_data['nota_fiscal_url'] = nota_fiscal_url

        return email_data
    except Exception as e:
        logging.error(f"Error aggregating email data for client {cliente_cpfCnpj} and ID {dados_id}: {e}")
        raise


def send_email(email_data, sg_client, retry_count=0):
    logging.info(f"Sending email with data: {email_data}")
    try:
        recipient_email = TEST_EMAIL if TEST_MODE else email_data.get('client_email')
        if not recipient_email:
            logging.warning(f"Email not sent. No email address for client {email_data.get('client_name')}.")
            return

        message = Mail(from_email=FROM_EMAIL, to_emails=recipient_email)
        message.template_id = TEMPLATE_ID
        message.dynamic_template_data = email_data

        asm = Asm(group_id=23816, groups_to_display=[23816, 23831, 23817])
        message.asm = asm
        logging.debug(f"ASM settings applied with group ID {asm.group_id} and groups to display: {asm.groups_to_display}")

        response = sg_client.send(message)

        if response.status_code in range(200, 300):
            logging.info(f"Email successfully sent to {recipient_email} on attempt {retry_count + 1}")
        else:
            logging.error(f"Failed to send email on attempt {retry_count + 1}: {response.status_code} | {response.body}")
            raise Exception("Email sending failed")

    except Exception as e:
        if retry_count < 2:
            wait_time = 2 ** (retry_count + 1) * 30
            logging.warning(f"Retrying to send email to {recipient_email}. Retry count: {retry_count + 1}. Waiting for {wait_time} seconds. Error: {e}")
            time.sleep(wait_time)
            send_email(email_data, sg_client, retry_count + 1)
        else:
            logging.error(f"Max retry attempts reached for sending email to {recipient_email}. Error: {e}")
