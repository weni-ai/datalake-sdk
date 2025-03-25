import grpc
import msgs_pb2
import msgs_pb2_grpc

SERVER_ADDRESS = "localhost:50051"

def send_data(path, data):
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = msgs_pb2_grpc.DatalakeManagerServiceStub(channel)

    request = msgs_pb2.InsertRequest(
        path=path,
        data=data
    )

    response = stub.InsertData(request)
    print("Server response:", response.status)
