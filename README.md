# z316_email_purchase

## Table of Contents

- [Features](#features)
- [Setup & Configuration](#setup--configuration)
  - [Prerequisites](#prerequisites)
  - [Dependencies](#dependencies)
  - [Environment Variables](#environment-variables)
- [Deployment](#deployment)
  - [Google Cloud Function Trigger](#google-cloud-function-trigger)
- [Usage](#usage)
  - [Detailed Process](#detailed-process)
- [SQL Queries](#sql-queries)
  - [SQL Query to Fetch Client Email](#sql-query-to-fetch-client-email)
  - [SQL Query for Purchase Details](#sql-query-for-purchase-details)
  - [SQL Query for Daily Checkins](#sql-query-for-daily-checkins)
  - [SQL Query for Trimester Spending](#sql-query-for-trimester-spending)
  - [SQL Query for Total Spend](#sql-query-for-total-spend)
- [Contributing](#contributing)
- [License](#license)

## Features

- **NFCe Generation:** Automatically generates NFCe for new sales using TinyERP's API.
- **Email Notification:** Sends detailed purchase details to clients, including NFCe links, using SendGrid.
- **Dynamic Data Retrieval:** Fetches client and purchase data from BigQuery and TinyERP API, with fallback mechanisms.
- **Resilient Data Fetching:** Implements a retry mechanism with exponential backoff for fetching purchase information, accommodating data pipeline delays.
- **Error Handling:** Implements robust error handling and logging mechanisms.

## Setup & Configuration

### Prerequisites

- Google Cloud Platform account
- Google Cloud Storage bucket set up to receive webhook payloads
- TinyERP and SendGrid accounts
- Configured Google Cloud Secret Manager for API tokens

### Dependencies

Install the required Python libraries with:

```bash
pip install -r requirements.txt
```

`requirements.txt` should include:

```txt
google-cloud-bigquery
google-cloud-secret-manager
google-cloud-storage
sendgrid
tenacity
requests
```

### Environment Variables

Set these environment variables in your Google Cloud Function configuration:

- `PROJECT_ID`: Your Google Cloud Project ID.
- `SECRET_MANAGER_API_TOKEN_NAME`: Secret in Secret Manager storing the TinyERP API token.
- `SECRET_MANAGER_SENDGRID_API_KEY_NAME`: Secret storing the SendGrid API key.
- `FROM_EMAIL`: Email address from which emails will be sent.
- `TEST_MODE`: Enable to send emails to a predefined address instead of the client's.
- `TEST_EMAIL`: Email address used in test mode.

## Deployment

Deploy to Google Cloud Functions, setting the trigger type to `Cloud Storage` and event type as `Finalize/Create`. Specify the bucket name holding the webhook payloads.

### Google Cloud Function Trigger

Set the trigger type to `Cloud Storage` and select the event type as `Finalize/Create`. Specify the bucket name holding the webhook payloads.

## Usage

The function is triggered automatically when a new JSON payload is added to the designated Cloud Storage bucket. The process includes:

1. **Payload Validation:** Checks for necessary keys in the payload.
2. **NFCe Generation:** Generates NFCe using TinyERP's API.
3. **Client Information Retrieval:** Attempts to fetch the client's email from BigQuery. Due to potential delays in the data pipeline, if the email isn't found, it falls back to fetching the information directly through the TinyERP API.
4. **Retry Mechanism:** For fetching purchase details, a retry mechanism with exponential backoff is used, allowing significant delays between retries to accommodate the data pipeline's processing time.
5. **Email Dispatch:** Sends an email with purchase details using SendGrid.

### Detailed Process

Upon receiving a new sale payload, the function first attempts to retrieve the client's contact information from BigQuery. Considering the BigQuery table is updated periodically throughout the day, there's a possibility that a client's registration might not be immediately reflected in BigQuery. In such cases, the function falls back to retrieving the client's contact information directly from the TinyERP API.

Additionally, when fetching client purchase information, the function employs a retry mechanism with an exponential backoff strategy. This is particularly useful for accommodating the latency inherent in data pipelines, where new purchase data might take some time to be reflected in BigQuery. The function allows for substantial delays between retries, ensuring that the data pipeline has adequate time to process and populate BigQuery with the latest purchase information before sending out the email.

## SQL Queries

The function utilizes several SQL queries to fetch necessary information from BigQuery:

### SQL Query to Fetch Client Email

```sql
SELECT email 
FROM `emporio-zingaro.z316_tiny.z316-tiny-contatos` 
WHERE cpf_cnpj = '{cpfCnpj}'
```

### SQL Query for Purchase Details

```sql
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
```

### SQL Query for Daily Checkins

```sql
SELECT
  COUNT(DISTINCT FORMAT_DATE('%Y-%m-%d', data)) AS daily_checkins
FROM
  `emporio-zingaro.z316_tiny_raw_json.pdv`
WHERE
  contato.cpfCnpj = '{cliente_cpfCnpj}'
  AND EXTRACT(QUARTER FROM data) = EXTRACT(QUARTER FROM CURRENT_DATE())
  AND EXTRACT(YEAR FROM data) = EXTRACT(YEAR FROM CURRENT_DATE());
```

### SQL Query for Trimester Spending

```sql
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
```

### SQL Query for Total Spend

```sql
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
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with your proposed changes or enhancements.

## License

This project is open-sourced under the MIT License. See the LICENSE file for more details.
