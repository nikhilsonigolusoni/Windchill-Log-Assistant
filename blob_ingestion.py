# blob_ingestion.py
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
 
class BlobLogIngestor:
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
        """Setup Azure Blob Storage client"""
        if not ACCOUNT_KEY:
            raise ValueError("Blob storage account key not found in configuration. Please check your .env file.")
           
        blob_conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={ACCOUNT_NAME};"
            f"AccountKey={ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
       
        logger.info(f"Connecting to Azure Blob Storage: {ACCOUNT_NAME}/{BLOB_CONTAINER}")
        return ContainerClient.from_connection_string(
            conn_str=blob_conn_str,
            container_name=BLOB_CONTAINER
        )
   
    def parse_log_entry(self, entry: Dict) -> Dict:
        """Extract required attributes from Application Insights log JSON."""
        return {
            "time": entry.get("time"),
            "Type": entry.get("Type"),
            "Properties": {
                "process": entry.get("Properties", {}).get("process"),
                "module": entry.get("Properties", {}).get("module"),
                "fileName": entry.get("Properties", {}).get("fileName"),
                "lineNumber": entry.get("Properties", {}).get("lineNumber"),
                "level": entry.get("Properties", {}).get("level"),
                "hostname": entry.get("Properties", {}).get("hostname"),
                "source": entry.get("Properties", {}).get("source"),
            },
            "Message": entry.get("Message"),
            "SeverityLevel": entry.get("SeverityLevel"),
            "ItemCount": entry.get("ItemCount"),
        }
   
    def fetch_logs_from_blob(self) -> List[Dict]:
        """Download all JSON logs from blob storage and parse them."""
        logs = []
        try:
            blob_list = list(self.container_client.list_blobs())
            logger.info(f"Found {len(blob_list)} blobs in container")
           
            for blob in tqdm(blob_list, desc="Downloading blobs"):
                if not blob.name.endswith(".json"):
                    logger.debug(f"Skipping non-JSON blob: {blob.name}")
                    continue
 
                logger.info(f"📥 Downloading {blob.name}")
                try:
                    downloader = self.container_client.download_blob(blob.name)
                    content = downloader.readall().decode("utf-8")
                   
                    line_count = 0
                    for line in content.splitlines():
                        try:
                            entry = json.loads(line.strip())
                            parsed = self.parse_log_entry(entry)
                            logs.append(parsed)
                            line_count += 1
                        except json.JSONDecodeError as e:
                            logger.debug(f"JSON decode error in {blob.name}: {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error parsing line in {blob.name}: {e}")
                            continue
                   
                    logger.info(f"✅ Processed {line_count} lines from {blob.name}")
                   
                except Exception as e:
                    logger.error(f"❌ Error processing blob {blob.name}: {e}")
                    continue
 
            logger.info(f"✅ Total logs loaded from blob storage: {len(logs)}")
           
        except Exception as e:
            logger.error(f"❌ Error fetching logs from blob: {e}")
            raise
           
        return logs
   
    def logs_to_documents(self, logs: List[Dict]) -> List[Document]:
        """Convert logs to LangChain Documents."""
        docs = []
        for log in tqdm(logs, desc="Converting logs to documents"):
            try:
                text = json.dumps(log, ensure_ascii=False)
                docs.append(
                    Document(
                        page_content=text,
                        metadata={
                            "time": log.get("time"),
                            "module": log.get("Properties", {}).get("module"),
                            "level": log.get("Properties", {}).get("level") or log.get("SeverityLevel"),
                            "source": "azure_blob",
                            "type": "app_insights_log",
                            "blob_source": "true"
                        },
                    )
                )
            except Exception as e:
                logger.warning(f"Error converting log to document: {e}")
                continue
        return docs
   
    def create_faiss_index(self, documents: List[Document], index_path: str) -> int:
        """Create FAISS index with batch processing."""
        if not documents:
            logger.error("No documents to index")
            return 0
           
        # Split documents first
        split_docs = self.text_splitter.split_documents(documents)
        logger.info(f"📊 Split {len(documents)} documents into {len(split_docs)} chunks")
       
        # Create FAISS index in batches to avoid memory issues
        batch_size = 500
        try:
            # Start with first batch
            first_batch = split_docs[:min(batch_size, len(split_docs))]
            logger.info(f"🔨 Creating initial index with {len(first_batch)} documents...")
           
            vector_store = FAISS.from_documents(first_batch, self.embeddings)
           
            # Add remaining documents in batches
            if len(split_docs) > batch_size:
                for i in tqdm(range(batch_size, len(split_docs), batch_size),
                            desc="Adding batches to index"):
                    batch = split_docs[i:i + batch_size]
                    try:
                        vector_store.add_documents(batch)
                        logger.info(f"✅ Added batch {i//batch_size + 1} to index")
                        time.sleep(1)  # Rate limiting
                    except Exception as e:
                        logger.error(f"❌ Error adding batch {i//batch_size + 1}: {e}")
                        continue
           
            # Save index
            vector_store.save_local(index_path)
            logger.info(f"💾 Index saved to {index_path}")
           
            return len(split_docs)
           
        except Exception as e:
            logger.error(f"❌ Error creating FAISS index: {e}")
            raise
   
    def ingest_blob_logs(self, index_path: str = FAISS_INDEX_PATH) -> int:
        """Main ingestion pipeline for blob storage logs."""
        logger.info("🚀 Starting blob storage log ingestion...")
        logger.info(f"Azure OpenAI Endpoint: {AZURE_OPENAI_ENDPOINT}")
        logger.info(f"Blob Storage: {ACCOUNT_NAME}/{BLOB_CONTAINER}")
       
        # Fetch logs from blob storage
        logs = self.fetch_logs_from_blob()
       
        if not logs:
            logger.error("❌ No logs found in blob storage!")
            return 0
       
        # Convert to documents
        documents = self.logs_to_documents(logs)
        logger.info(f"📄 Created {len(documents)} documents")
       
        if not documents:
            logger.error("❌ No documents created from logs!")
            return 0
       
        # Create FAISS index
        count = self.create_faiss_index(documents, index_path)
       
        logger.info(f"🎯 Ingestion complete! Processed {count} document chunks.")
        return count
 
def main():
    """Main function to run the ingestion process."""
    try:
        ingestor = BlobLogIngestor()
        count = ingestor.ingest_blob_logs()
       
        if count > 0:
            print(f"✅ Success! Processed {count} document chunks.")
            print(f"📁 FAISS index saved to: {FAISS_INDEX_PATH}")
        else:
            print("❌ No documents were processed.")
           
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("❌ Please check your Azure Blob Storage configuration in the .env file")
        print("❌ Make sure BLOB_ACCOUNT_KEY is set in your .env file")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        print("❌ Ingestion failed. Please check the logs for details.")
 
if __name__ == "__main__":
    main()