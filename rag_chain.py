# rag_chain.py
import os
from langchain_community.vectorstores import FAISS
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from config import *

class WindchillRAG:
    def __init__(self, windchill_index_path: str = FAISS_INDEX_PATH, 
                 http_index_path: str = FAISS_HTTP_INDEX_PATH):
        
        # Initialize embeddings
        self.embeddings = AzureOpenAIEmbeddings(
            azure_deployment=EMBEDDING_DEPLOYMENT,
            openai_api_version="2023-05-15",
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY
        )
       
        # Load both vector stores
        try:
            self.windchill_vector_store = FAISS.load_local(
                windchill_index_path,
                self.embeddings,
                allow_dangerous_deserialization=True
            )
            print(f"✅ Loaded Windchill FAISS index from {windchill_index_path}")
        except Exception as e:
            print(f"❌ Error loading Windchill FAISS index: {e}")
            raise
        
        try:
            self.http_vector_store = FAISS.load_local(
                http_index_path,
                self.embeddings,
                allow_dangerous_deserialization=True
            )
            print(f"✅ Loaded HTTP FAISS index from {http_index_path}")
        except Exception as e:
            print(f"❌ Error loading HTTP FAISS index: {e}")
            raise
       
        # Initialize LLM
        self.llm = AzureChatOpenAI(
            azure_deployment=CHAT_DEPLOYMENT,
            openai_api_version="2023-05-15",
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_KEY,
            temperature=0.1,
            max_tokens=1000
        )
        
        # Create retrievers
        self.windchill_retriever = self.windchill_vector_store.as_retriever(
            search_type="similarity", search_kwargs={"k": 5}
        )
        
        self.http_retriever = self.http_vector_store.as_retriever(
            search_type="similarity", search_kwargs={"k": 5}
        )
       
        # Create custom prompts for different log types
        self.combined_prompt_template = PromptTemplate(
            template="""You are a Windchill log analysis expert. Analyze both application logs and HTTP access logs to provide actionable insights.

WINDCHILL APPLICATION LOGS CONTEXT:
{windchill_context}

HTTP ACCESS LOGS CONTEXT:
{http_context}

USER QUESTION: {question}

Please analyze both log types and provide:
1. Key issues or errors found with severity levels
2. Root cause analysis for identified problems
3. Specific remediation steps and fixes
4. Impact assessment on system performance
5. Preventive measures for future

Format your response with clear sections and bullet points. Focus on actionable recommendations.

If you need to reference specific log entries, mention their key characteristics.

ANSWER:""",
            input_variables=["windchill_context", "http_context", "question"]
        )
        
        self.windchill_prompt_template = PromptTemplate(
            template="""You are a Windchill application log analysis expert. Analyze the application logs and provide specific fixes.

CONTEXT FROM APPLICATION LOGS:
{context}

USER QUESTION: {question}

Please provide:
1. Error patterns and their frequencies
2. Module-specific issues and root causes
3. Immediate remediation steps
4. Long-term preventive measures
5. Impact on system functionality

Focus on practical solutions that operations teams can implement.

ANSWER:""",
            input_variables=["context", "question"]
        )
        
        self.http_prompt_template = PromptTemplate(
            template="""You are a Windchill HTTP log analysis expert. Analyze the HTTP access logs for performance and issues.

CONTEXT FROM HTTP LOGS:
{context}

USER QUESTION: {question}

Please analyze:
1. Performance bottlenecks and slow endpoints
2. HTTP error patterns (4xx, 5xx status codes)
3. Client behavior and usage patterns
4. API endpoint performance issues
5. Security and authentication problems

Provide specific optimization recommendations and fixes.

ANSWER:""",
            input_variables=["context", "question"]
        )
        
        # Remediation-specific prompt
        self.remediation_prompt_template = PromptTemplate(
            template="""You are a Windchill system remediation expert. Analyze the logs and create an actionable fix plan.

CONTEXT:
{context}

USER REQUEST: {question}

Create a comprehensive remediation report including:

CRITICAL ISSUES:
- List critical issues with severity levels
- Root cause analysis for each issue
- Immediate fixes required

PERFORMANCE OPTIMIZATIONS:
- Identify performance bottlenecks
- Suggest specific optimizations
- Estimated impact of each fix

SECURITY RECOMMENDATIONS:
- Security vulnerabilities found
- Authentication/authorization issues
- Security hardening recommendations

ACTION PLAN:
- Priority order for fixes
- Estimated time for each remediation
- Team responsibilities if applicable

Prefer specific, actionable recommendations over general advice.

REMEDIATION REPORT:""",
            input_variables=["context", "question"]
        )
   
    def query(self, question: str, log_type: str = "combined"):
        """Query the RAG system with a question"""
        try:
            if log_type == "combined":
                # Get documents from both sources
                windchill_docs = self.windchill_retriever.get_relevant_documents(question)
                http_docs = self.http_retriever.get_relevant_documents(question)
                
                windchill_context = "\n\n".join([doc.page_content for doc in windchill_docs])
                http_context = "\n\n".join([doc.page_content for doc in http_docs])
                
                # Use combined prompt
                prompt = self.combined_prompt_template.format(
                    windchill_context=windchill_context,
                    http_context=http_context,
                    question=question
                )
                
                result = self.llm.invoke(prompt)
                source_docs = windchill_docs + http_docs
                
            elif log_type == "windchill":
                # Use only windchill logs
                qa_chain = RetrievalQA.from_chain_type(
                    llm=self.llm,
                    chain_type="stuff",
                    retriever=self.windchill_retriever,
                    chain_type_kwargs={"prompt": self.windchill_prompt_template},
                    return_source_documents=True
                )
                result = qa_chain.invoke({"query": question})
                source_docs = result.get("source_documents", [])
                
            else:  # http logs
                # Use only http logs
                qa_chain = RetrievalQA.from_chain_type(
                    llm=self.llm,
                    chain_type="stuff",
                    retriever=self.http_retriever,
                    chain_type_kwargs={"prompt": self.http_prompt_template},
                    return_source_documents=True
                )
                result = qa_chain.invoke({"query": question})
                source_docs = result.get("source_documents", [])
            
            return {
                "result": result["result"] if isinstance(result, dict) else result.content,
                "source_documents": source_docs,
                "log_type": log_type
            }
        except Exception as e:
            return {
                "result": f"Error processing query: {str(e)}",
                "source_documents": [],
                "log_type": log_type
            }
    
    def generate_remediation_report(self, question: str = "Generate comprehensive remediation report"):
        """Generate a detailed remediation report"""
        try:
            # Get documents from both sources for comprehensive analysis
            windchill_docs = self.windchill_retriever.get_relevant_documents(question)
            http_docs = self.http_retriever.get_relevant_documents(question)
            
            all_docs = windchill_docs + http_docs
            context = "\n\n".join([doc.page_content for doc in all_docs])
            
            # Use remediation-specific prompt
            prompt = self.remediation_prompt_template.format(
                context=context,
                question=question
            )
            
            result = self.llm.invoke(prompt)
            
            return {
                "result": result.content,
                "source_documents": all_docs,
                "log_type": "combined"
            }
            
        except Exception as e:
            return {
                "result": f"Error generating remediation report: {str(e)}",
                "source_documents": [],
                "log_type": "combined"
            }

# Optional: Add a simple test function
if __name__ == "__main__":
    # Test the RAG system
    try:
        rag = WindchillRAG()
        print("✅ RAG system initialized successfully!")
        
        # Test query
        test_result = rag.query("What are the common errors in the system?", "combined")
        print("✅ Test query executed successfully!")
        print(f"Response length: {len(test_result['result'])} characters")
        print(f"Source documents: {len(test_result['source_documents'])}")
        
    except Exception as e:
        print(f"❌ Error initializing RAG system: {e}")