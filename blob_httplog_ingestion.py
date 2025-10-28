# blob_httplog_ingestion.py
import os
import json
import time
from typing import List, Dict
from azure.storage.blob import ContainerClient
from openai import RateLimitError

from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.docstore.document import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from tqdm import tqdm
import logging

from config import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BlobHttpLogIngestor:
    def __init__(self):
        self.embeddings = AzureOpenAIEmbeddings(
            azure_deployment=EMBEDDING_DEPLOYMENT,
            openai_api_version="2023-05-15",
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
            chunk_size=16,
            max_retries=3,
            timeout=30
        )
       
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=100,
            length_function=len
        )
       
        self.container_client = self._setup_blob_client()
   
    def _setup_blob_client(self):
        """Setup Azure Blob Storage client for HTTP logs"""
        if not HTTP_ACCOUNT_KEY:
            raise ValueError("HTTP Blob storage account key not found in configuration. Please check your .env file.")
           
        blob_conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={HTTP_ACCOUNT_NAME};"
            f"AccountKey={HTTP_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
       
        logger.info(f"Connecting to Azure Blob Storage: {HTTP_ACCOUNT_NAME}/{HTTP_BLOB_CONTAINER}")
        return ContainerClient.from_connection_string(
            conn_str=blob_conn_str,
            container_name=HTTP_BLOB_CONTAINER
        )
   
    def parse_http_log_entry(self, entry: Dict) -> Dict:
        """Extract required attributes from HTTP log JSON."""
        properties = entry.get("Properties", {})
        return {
            "time": entry.get("time"),
            "Type": entry.get("Type"),
            "Properties": {
                "process": properties.get("process"),
                "module": properties.get("module"),
                "fileName": properties.get("fileName"),
                "lineNumber": properties.get("lineNumber"),
                "level": properties.get("level"),
                "client_ip": properties.get("client_ip"),
                "user": properties.get("user"),
                "timestamp": properties.get("timestamp"),
                "method": properties.get("method"),
                "url": properties.get("url"),
                "protocol": properties.get("protocol"),
                "status": properties.get("status"),
                "size": properties.get("size"),
                "response_time": properties.get("response_time"),  # in milliseconds
                "hostname": properties.get("hostname"),
                "source": properties.get("source"),
            },
            "Message": entry.get("Message"),
            "SeverityLevel": entry.get("SeverityLevel"),
            "ItemCount": entry.get("ItemCount"),
            "ClientIP": entry.get("ClientIP"),
            "ClientCountryOrRegion": entry.get("ClientCountryOrRegion"),
            "ClientCity": entry.get("ClientCity"),
            "ClientBrowser": entry.get("ClientBrowser"),
        }
   
    def fetch_http_logs_from_blob(self) -> List[Dict]:
        """Download all JSON HTTP logs from blob storage and parse them."""
        logs = []
        try:
            blob_list = list(self.container_client.list_blobs())
            logger.info(f"Found {len(blob_list)} HTTP log blobs in container")
           
            for blob in tqdm(blob_list, desc="Downloading HTTP log blobs"):
                if not blob.name.endswith(".json"):
                    logger.debug(f"Skipping non-JSON blob: {blob.name}")
                    continue

                logger.info(f"📥 Downloading HTTP log {blob.name}")
                try:
                    downloader = self.container_client.download_blob(blob.name)
                    content = downloader.readall().decode("utf-8")
                   
                    line_count = 0
                    for line in content.splitlines():
                        try:
                            entry = json.loads(line.strip())
                            parsed = self.parse_http_log_entry(entry)
                            logs.append(parsed)
                            line_count += 1
                        except json.JSONDecodeError as e:
                            logger.debug(f"JSON decode error in {blob.name}: {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error parsing line in {blob.name}: {e}")
                            continue
                   
                    logger.info(f"✅ Processed {line_count} HTTP log lines from {blob.name}")
                   
                except Exception as e:
                    logger.error(f"❌ Error processing HTTP log blob {blob.name}: {e}")
                    continue

            logger.info(f"✅ Total HTTP logs loaded from blob storage: {len(logs)}")
           
        except Exception as e:
            logger.error(f"❌ Error fetching HTTP logs from blob: {e}")
            raise
           
        return logs
   
    def http_logs_to_documents(self, logs: List[Dict]) -> List[Document]:
        """Convert HTTP logs to LangChain Documents."""
        docs = []
        for log in tqdm(logs, desc="Converting HTTP logs to documents"):
            try:
                # Create a more descriptive text content for HTTP logs
                properties = log.get("Properties", {})
                text_content = f"""
HTTP Request: {properties.get('method')} {properties.get('url')}
Status: {properties.get('status')}
Response Time: {properties.get('response_time')}ms
Client IP: {properties.get('client_ip')}
User: {properties.get('user')}
Timestamp: {properties.get('timestamp')}
Hostname: {properties.get('hostname')}
Message: {log.get('Message')}
                """.strip()
               
                docs.append(
                    Document(
                        page_content=text_content,
                        metadata={
                            "time": log.get("time"),
                            "timestamp": properties.get("timestamp"),
                            "method": properties.get("method"),
                            "url": properties.get("url"),
                            "status": properties.get("status"),
                            "response_time": properties.get("response_time"),
                            "client_ip": properties.get("client_ip"),
                            "user": properties.get("user"),
                            "hostname": properties.get("hostname"),
                            "source": "azure_blob_http",
                            "type": "http_access_log",
                            "level": properties.get("level", "INFO"),
                            "module": properties.get("module"),
                            "blob_source": "true"
                        },
                    )
                )
            except Exception as e:
                logger.warning(f"Error converting HTTP log to document: {e}")
                continue
        return docs
   
    def create_faiss_index(self, documents: List[Document], index_path: str) -> int:
        """Create FAISS index with batch processing for HTTP logs."""
        if not documents:
            logger.error("No HTTP log documents to index")
            return 0
           
        # Split documents first
        split_docs = self.text_splitter.split_documents(documents)
        logger.info(f"📊 Split {len(documents)} HTTP log documents into {len(split_docs)} chunks")
       
        # Create FAISS index in batches to avoid memory issues
        batch_size = 500
        try:
            # Start with first batch
            first_batch = split_docs[:min(batch_size, len(split_docs))]
            logger.info(f"🔨 Creating initial HTTP log index with {len(first_batch)} documents...")
           
            vector_store = FAISS.from_documents(first_batch, self.embeddings)
           
            # Add remaining documents in batches
            if len(split_docs) > batch_size:
                for i in tqdm(range(batch_size, len(split_docs), batch_size),
                            desc="Adding HTTP log batches to index"):
                    batch = split_docs[i:i + batch_size]
                    try:
                        vector_store.add_documents(batch)
                        logger.info(f"✅ Added HTTP log batch {i//batch_size + 1} to index")
                        time.sleep(1)  # Rate limiting
                    except Exception as e:
                        logger.error(f"❌ Error adding HTTP log batch {i//batch_size + 1}: {e}")
                        continue
           
            # Save index
            vector_store.save_local(index_path)
            logger.info(f"💾 HTTP log index saved to {index_path}")
           
            return len(split_docs)
           
        except Exception as e:
            logger.error(f"❌ Error creating HTTP log FAISS index: {e}")
            raise
   
    def ingest_http_logs(self, index_path: str = FAISS_HTTP_INDEX_PATH) -> int:
        """Main ingestion pipeline for HTTP logs."""
        logger.info("🚀 Starting HTTP log ingestion...")
        logger.info(f"Azure OpenAI Endpoint: {AZURE_OPENAI_ENDPOINT}")
        logger.info(f"HTTP Blob Storage: {HTTP_ACCOUNT_NAME}/{HTTP_BLOB_CONTAINER}")
       
        # Fetch HTTP logs from blob storage
        logs = self.fetch_http_logs_from_blob()
       
        if not logs:
            logger.error("❌ No HTTP logs found in blob storage!")
            return 0
       
        # Convert to documents
        documents = self.http_logs_to_documents(logs)
        logger.info(f"📄 Created {len(documents)} HTTP log documents")
       
        if not documents:
            logger.error("❌ No documents created from HTTP logs!")
            return 0
       
        # Create FAISS index
        count = self.create_faiss_index(documents, index_path)
       
        logger.info(f"🎯 HTTP log ingestion complete! Processed {count} document chunks.")
        return count

def main():
    """Main function to run the HTTP log ingestion process."""
    try:
        ingestor = BlobHttpLogIngestor()
        count = ingestor.ingest_http_logs()
       
        if count > 0:
            print(f"✅ Success! Processed {count} HTTP log document chunks.")
            print(f"📁 FAISS index saved to: {FAISS_HTTP_INDEX_PATH}")
        else:
            print("❌ No HTTP log documents were processed.")
           
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("❌ Please check your HTTP Blob Storage configuration in the .env file")
        print("❌ Make sure HTTP_BLOB_ACCOUNT_KEY is set in your .env file")
    except Exception as e:
        logger.error(f"HTTP log ingestion failed: {e}")
        print("❌ HTTP log ingestion failed. Please check the logs for details.")

if __name__ == "__main__":
    main()