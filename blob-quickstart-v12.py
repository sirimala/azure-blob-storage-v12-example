import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


def upload_blob(container_name, blob_service_client):
    try:
        # Create a file in local data directory to upload and download
        local_path = "./data"
        # local_file_name = "quickstart" + str(uuid.uuid4()) + ".txt" # generates new filename everytime
        local_file_name = "quickstart4bae9e94-8c84-499f-bcb6-8a7cc1abe836.txt"
        upload_file_path = os.path.join(local_path, local_file_name)

        # Write text to the file
        file = open(upload_file_path, 'w')
        file.write("Hello, World!")
        file.close()

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)
    except Exception as ex:
        print('Exception:')
        print(ex)


def list_blobs_in_container(container_name, blob_service_client):

    # Get a client to interact with a specific container - though it may not yet exist
    container_client = blob_service_client.get_container_client(container_name)
    try:
        print("\nListing blobs...")

        # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)
            print(blob.metadata)
            print(blob.container)
            print(blob.snapshot)
            print(dir(blob))
            download_blob(blob, blob_service_client)
    except Exception as ex:
        print('Exception:')
        print(ex)


def download_blob(blob, blob_service_client):
    try:

        local_path = "./data"
        # local_file_name = "quickstart" + str(uuid.uuid4()) + ".txt" # generates new filename everytime
        local_file_name = blob.name

        # Download the blob to a local file
        # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
        download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.txt'))
        print("\nDownloading blob to \n\t" + download_file_path)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=blob.container, blob=local_file_name)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
    except Exception as ex:
        print('Exception:')
        print(ex)


def delete_container(container_client):
    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(upload_file_path)
    os.remove(download_file_path)

    print("Done")

def main():
    try:
        print('Azure Blob storage v' + __version__ + ' - Python quickstart sample')
        # Quick start code goes here

        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a unique name for the container
        # container_name = "quickstart" + str(uuid.uuid4()) # creates a new container name every run
        container_name = "quickstartd6247b46-ccf9-49bd-a627-1ef057f772e0"


        # # Create the container
        # container_client = blob_service_client.create_container(container_name)

        list_blobs_in_container(container_name, blob_service_client)
        # download_blob(container_name, blob_service_client)
    except Exception as ex:
        print('Exception:')
        print(ex)


if __name__ == '__main__':
    main()