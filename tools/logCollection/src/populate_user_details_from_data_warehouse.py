from google.cloud import bigquery
from google.cloud import secretmanager

def populate_user_details(populated_new_logs, emails):
    # Get user details from Thurloe/data warehouse
    bq_client = bigquery.Client()
    sm_client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret.
    name = f"projects/terra-datarepo-production/secrets/warehouse-db-name/versions/latest"
    response = sm_client.access_secret_version(request={"name": name})
    WAREHOUSE_DB_NAME = response.payload.data.decode("UTF-8")

    # # Perform a query.
    QUERY = (
        f"""
        SELECT t.USER_ID, e.email, t.Key, t.Value FROM `{WAREHOUSE_DB_NAME}.thurloe` t
       JOIN `{WAREHOUSE_DB_NAME}.email` e ON t.USER_ID = e.USER_ID
        WHERE t.KEY IN ("firstName", "lastName", "institute", "programLocationCountry")
        AND t.USER_ID IN(
            SELECT USER_ID FROM `{WAREHOUSE_DB_NAME}.email`
            WHERE email IN UNNEST(@emails)
        )
        ORDER BY t.USER_ID
        """
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("emails", "STRING", emails)
        ]
    )

    query_job = bq_client.query(QUERY, job_config=job_config)  # API request
    rows = query_job.result()  # Waits for query to finish

    first_names = {}
    last_names = {}
    institutes = {}
    countries = {}
    user_ids = {}
    for row in rows:
        user_ids[row.email] = row.USER_ID
        if (row.Key == "firstName"):
            first_names[row.email] = row.Value
        elif (row.Key == "lastName"):
            last_names[row.email] = row.Value
        elif (row.Key == "institute"):
            institutes[row.email] = row.Value
        elif (row.Key == "programLocationCountry"):
            countries[row.email] = row.Value

    for log in populated_new_logs:
        if log.user_email != "N/A":
            if log.user_email in first_names:
                log.user_name = first_names[log.user_email] + " " + last_names[log.user_email]
                log.user_country_name = countries[log.user_email]
                log.user_id = user_ids[log.user_email]
                if institutes[log.user_email] is not None:
                    log.user_org = institutes[log.user_email]
            else:
                log.user_name = ""
                log.user_country_name = ""
