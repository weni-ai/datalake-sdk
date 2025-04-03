import grpc

from weni_datalake_sdk.clients import (
    msgs_pb2,
    msgs_pb2_grpc,
    traces_pb2,
    traces_pb2_grpc,
)
from weni_datalake_sdk.paths.validator import validate_path

# from django.conf import settings


# SERVER_ADDRESS = settings.DATALAKE_SERVER_ADDRESS
SERVER_ADDRESS = "localhost:50051"  # Replace with your actual server address


def send_data(path, data):
    channel = grpc.insecure_channel(target=SERVER_ADDRESS)
    stub = msgs_pb2_grpc.DatalakeManagerServiceStub(channel)

    if isinstance(path, type):
        path = path()

    validate_path(path)

    request = msgs_pb2.InsertRequest(path=path.get_table_name(), data=data)

    response = stub.InsertData(request)
    print("Server response:", response.status)


def send_trace_data(path_class, data):
    validate_path(path_class)

    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = traces_pb2_grpc.DatalakeManagerServiceStub(channel)

    request = traces_pb2.InsertTraceRequest(path=path_class.get_table_name(), data=data)

    response = stub.InsertTraceData(request)
    return response.status
