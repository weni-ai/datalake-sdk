import json
import os

import boto3
import requests

REDSHIFT_QUERY_BASE_URL = os.environ.get("REDSHIFT_QUERY_BASE_URL")


def query_redshift(metric: str, query_params: dict = None) -> dict:
    """
    Envia uma query SQL para o endpoint do Redshift e retorna os dados.
    """
    if not REDSHIFT_QUERY_BASE_URL:
        raise EnvironmentError("Missing REDSHIFT_QUERY_BASE_URL env variable")

    url = f"{REDSHIFT_QUERY_BASE_URL.rstrip('/')}/{metric}"

    token = get_secrets()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = query_params or {}

    response = requests.post(url, headers=headers, json=payload)

    if not response.ok:
        raise Exception(f"Query failed [{response.status_code}]: {response.text}")

    return response.json()


def get_secrets():
    SECRET_NAME = os.environ.get("REDSHIFT_SECRET_NAME")
    if not SECRET_NAME:
        raise EnvironmentError("Missing REDSHIFT_SECRET_NAME env variable")

    client_sts = boto3.client("sts")
    credentials = client_sts.assume_role(
        RoleArn=os.environ.get("REDSHIFT_ROLE_ARN"), RoleSessionName="be-another-me"
    )["Credentials"]

    SECRETS_CLIENT = boto3.client(
        "secretsmanager",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    try:
        current_secrets = SECRETS_CLIENT.get_secret_value(
            SecretId=SECRET_NAME, VersionStage="AWSCURRENT"
        )
        current_dict = json.loads(current_secrets["SecretString"])
        return current_dict["token"]
    except Exception as e:
        raise Exception(e)
