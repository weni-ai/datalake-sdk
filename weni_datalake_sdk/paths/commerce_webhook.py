from weni_datalake_sdk.paths.base_path import BasePath


class CommerceWebhookPath(BasePath):
    TABLE_NAME = "commerce_webhook"

    @classmethod
    def get_table_name(cls):
        return cls.TABLE_NAME
