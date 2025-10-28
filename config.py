# config.py
import os
from dotenv import load_dotenv
 
load_dotenv()
 
# Azure OpenAI Config
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://genaiwindchillplm.cognitiveservices.azure.com/")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
EMBEDDING_DEPLOYMENT = os.getenv("EMBEDDING_DEPLOYMENT", "text-embedding-3-small")
CHAT_DEPLOYMENT = os.getenv("CHAT_DEPLOYMENT", "gpt-4.1-mini")
 
# Azure Blob Storage Config
ACCOUNT_NAME = os.getenv("BLOB_ACCOUNT_NAME", "wcgenaistorage001")
ACCOUNT_KEY = os.getenv("BLOB_ACCOUNT_KEY")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "insights-logs-apptraces")
 
# Processing Config
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "800"))
FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "faiss_blob_logs")

# HTTP Logs Configuration
HTTP_ACCOUNT_NAME = os.getenv("HTTP_BLOB_ACCOUNT_NAME")
HTTP_ACCOUNT_KEY = os.getenv("HTTP_BLOB_ACCOUNT_KEY")
HTTP_BLOB_CONTAINER = os.getenv("HTTP_BLOB_CONTAINER", "insights-logs-apptraces")
FAISS_HTTP_INDEX_PATH = os.getenv("FAISS_HTTP_INDEX_PATH", "faiss_http_index")



####################################################

