from django.conf import settings
import grpc
from weni_datalake_sdk.clients import msgs_pb2, msgs_pb2_grpc
from weni_datalake_sdk.paths.validator import validate_path

# SERVER_ADDRESS = "localhost:50051"
SERVER_ADDRESS = settings.DATALAKE_SERVER_ADDRESS

def send_data(path, data):
    channel = grpc.insecure_channel(target=SERVER_ADDRESS)
    stub = msgs_pb2_grpc.DatalakeManagerServiceStub(channel)

    if isinstance(path, type):
        path = path()

    validate_path(path)

    request = msgs_pb2.InsertRequest(
        path=path.get_table_name(),
        data=data
    )

    response = stub.InsertData(request)
    print("Server response:", response.status)
