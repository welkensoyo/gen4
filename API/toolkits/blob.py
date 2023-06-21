from azure.storage.blob import BlobClient

class AzureBlob:
    def __init__(self, blob):
        self.account = 'https://sdbprodstacc01.blob.core.windows.net/'
        self.container = 'transactionscontainer'
        self.blob = blob
        self.creds = 'Nk5G7QvnQJsOdXBYAqLiMV7I0MM24k4prCHF0ag4NobtpCu/q59I4KNH0tek7PWKCDyFE442/xk8+AStSjHmLw=='

    def blob_conn(self):
        blob = BlobClient(account_url=self.account,
                          container_name=self.container,
                          blob_name=self.blob,
                          credential=self.creds)
        return blob

