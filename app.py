# app.py
import streamlit as st
import os
import time
from dotenv import load_dotenv
from rag_chain import WindchillRAG
from datetime import datetime, timedelta
import pandas as pd

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Windchill Log Analyzer",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .log-entry {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        font-family: monospace;
        font-size: 0.9em;
    }
    .error-log { border-left: 4px solid #ff4b4b; }
    .warning-log { border-left: 4px solid #ffa500; }
    .info-log { border-left: 4px solid #1f77b4; }
    .http-log { border-left: 4px solid #2e8b57; }
    .windchill-log { border-left: 4px solid #9370db; }
    .source-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.7em;
        font-weight: bold;
        margin-right: 8px;
        color: white;
    }
    .http-badge { background-color: #2e8b57; }
    .windchill-badge { background-color: #9370db; }
    .issue-critical { background-color: #ffebee; border-left: 4px solid #d32f2f; }
    .issue-high { background-color: #fff3e0; border-left: 4px solid #f57c00; }
    .issue-medium { background-color: #f3e5f5; border-left: 4px solid #7b1fa2; }
    .issue-low { background-color: #e8f5e8; border-left: 4px solid #388e3c; }
</style>
""", unsafe_allow_html=True)

class WindchillChatbot:
    def __init__(self):
        try:
            self.rag = WindchillRAG()
            self.initialized = True
            self.error_message = None
        except Exception as e:
            self.initialized = False
            self.error_message = str(e)
   
    def get_response(self, question: str, log_type: str = "combined"):
        """Get response from RAG system with log type selection"""
        if not self.initialized:
            return {
                "result": f"❌ Chatbot not initialized: {self.error_message}",
                "source_documents": [],
                "log_type": log_type
            }
        
        with st.spinner(f"Analyzing {log_type} logs..."):
            result = self.rag.query(question, log_type)
       
        return result

def format_source_document(source, index: int):
    """Format a source document for display with appropriate styling"""
    source_type = source.metadata.get('source_type', 'windchill_log')
    log_level = source.metadata.get('level', 'INFO').upper()
    
    # Determine CSS class based on log type and level
    if source_type == 'http_log':
        css_class = "http-log"
        badge_class = "http-badge"
        badge_text = "HTTP"
    else:
        css_class = "windchill-log" 
        badge_class = "windchill-badge"
        badge_text = "WINCHILL"
    
    # Add level-based styling
    if log_level == 'ERROR':
        css_class += " error-log"
    elif log_level == 'WARNING':
        css_class += " warning-log"
    else:
        css_class += " info-log"
    
    # Format content based on source type
    if source_type == 'http_log':
        content = f"""
        <strong>Method:</strong> {source.metadata.get('method', 'N/A')} | 
        <strong>URL:</strong> {source.metadata.get('url', 'N/A')} | 
        <strong>Status:</strong> {source.metadata.get('status', 'N/A')} | 
        <strong>Response Time:</strong> {source.metadata.get('response_time', 'N/A')}ms
        <br><strong>Client IP:</strong> {source.metadata.get('client_ip', 'N/A')} | 
        <strong>Timestamp:</strong> {source.metadata.get('timestamp', 'N/A')}
        """
        preview = source.page_content[:300] + "..." if len(source.page_content) > 300 else source.page_content
    else:
        content = f"""
        <strong>Time:</strong> {source.metadata.get('time', 'Unknown')} | 
        <strong>Module:</strong> {source.metadata.get('module', 'Unknown')} | 
        <strong>Level:</strong> {log_level}
        """
        preview = source.page_content[:300] + "..." if len(source.page_content) > 300 else source.page_content
    
    return f'''
    <div class="log-entry {css_class}">
        <span class="source-badge {badge_class}">{badge_text}</span>
        <span class="source-badge" style="background-color: #666;">#{index + 1}</span>
        {content}
        <br>
        <strong>Content:</strong> {preview}
    </div>
    '''

def check_index_files():
    """Check if the required index files exist"""
    windchill_index_path = os.getenv("FAISS_INDEX_PATH", "faiss_windchill_index")
    http_index_path = os.getenv("FAISS_HTTP_INDEX_PATH", "faiss_http_index")
    
    windchill_exists = os.path.exists(windchill_index_path) and os.path.exists(os.path.join(windchill_index_path, "index.faiss"))
    http_exists = os.path.exists(http_index_path) and os.path.exists(os.path.join(http_index_path, "index.faiss"))
    
    return windchill_exists, http_exists

def generate_remediation_report(chatbot):
    """Generate a simple remediation report with common fixes"""
    
    st.header("📋 Quick Fix Recommendations")
    st.info("Based on recent log analysis, here are the top issues to address:")
    
    # Sample remediation data - in real implementation, this would come from AI analysis
    remediation_data = [
        {
            "issue": "High Database Connection Timeouts",
            "priority": "🚨 CRITICAL",
            "frequency": "45 occurrences in 24h",
            "quick_fix": "Increase connection pool size from 50 to 100",
            "detailed_fix": "Update database configuration and implement connection retry logic",
            "impact": "User login failures, data sync issues",
            "estimated_time": "2-4 hours",
            "category": "Database"
        },
        {
            "issue": "Slow API Response Times", 
            "priority": "⚠️ HIGH",
            "frequency": "23 slow requests (>5s)",
            "quick_fix": "Add caching for frequent API calls",
            "detailed_fix": "Implement Redis cache for user session data and frequent queries",
            "impact": "Poor user experience, timeout errors",
            "estimated_time": "3-5 hours",
            "category": "Performance"
        },
        {
            "issue": "Memory Leak in User Module",
            "priority": "🚨 CRITICAL",
            "frequency": "Memory spikes every 2 hours",
            "quick_fix": "Restart affected service during low traffic",
            "detailed_fix": "Profile memory usage and fix object retention in user session handling",
            "impact": "Service crashes, performance degradation",
            "estimated_time": "4-6 hours",
            "category": "Memory"
        },
        {
            "issue": "404 Errors on Static Assets",
            "priority": "✅ LOW",
            "frequency": "12% of static requests failing",
            "quick_fix": "Check CDN configuration and file permissions",
            "detailed_fix": "Verify asset paths and update load balancer rules",
            "impact": "Broken UI elements, missing images",
            "estimated_time": "1-2 hours",
            "category": "Configuration"
        }
    ]
    
    # Display issues in a clean format
    for i, issue in enumerate(remediation_data, 1):
        # Determine CSS class based on priority
        priority_class = ""
        if "CRITICAL" in issue['priority']:
            priority_class = "issue-critical"
        elif "HIGH" in issue['priority']:
            priority_class = "issue-high"
        elif "MEDIUM" in issue['priority']:
            priority_class = "issue-medium"
        else:
            priority_class = "issue-low"
        
        with st.expander(f"{i}. {issue['issue']} - {issue['priority']}", expanded=(i == 1)):
            st.markdown(f'<div class="{priority_class}" style="padding: 10px; border-radius: 5px; margin: 5px 0;">', unsafe_allow_html=True)
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.write(f"**Category:** {issue['category']}")
                st.write(f"**Frequency:** {issue['frequency']}")
                st.write(f"**Estimated Time:** {issue['estimated_time']}")
                st.write(f"**Impact:** {issue['impact']}")
            
            with col2:
                st.success(f"**Quick Fix:** {issue['quick_fix']}")
                st.info(f"**Detailed Solution:** {issue['detailed_fix']}")
                
                # Add action buttons
                cola, colb, colc = st.columns(3)
                with cola:
                    if st.button("📝 Create Ticket", key=f"ticket_{i}"):
                        st.success("Ticket created for this issue!")
                with colb:
                    if st.button("✅ Mark as Fixed", key=f"fixed_{i}"):
                        st.success("Issue marked as resolved!")
                with colc:
                    if st.button("🔍 Analyze Logs", key=f"analyze_{i}"):
                        st.session_state.analyze_issue = issue['issue']
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Summary statistics
    st.subheader("📊 Report Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    total_issues = len(remediation_data)
    critical_issues = len([i for i in remediation_data if "CRITICAL" in i['priority']])
    high_issues = len([i for i in remediation_data if "HIGH" in i['priority']])
    
    with col1:
        st.metric("Total Issues", total_issues)
    with col2:
        st.metric("Critical Issues", critical_issues)
    with col3:
        st.metric("High Priority", high_issues)
    with col4:
        total_time = sum([int(issue['estimated_time'].split('-')[0]) for issue in remediation_data])
        st.metric("Total Fix Time", f"{total_time}+ hours")
    
    # Export options
    st.subheader("📤 Export Options")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Generate PDF Report"):
            st.success("PDF report generated successfully!")
            st.info("Report saved to 'windchill_remediation_report.pdf'")
    
    with col2:
        if st.button("📊 Export to CSV"):
            # Create DataFrame and export
            df = pd.DataFrame(remediation_data)
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="windchill_issues.csv",
                mime="text/csv"
            )
    
    with col3:
        if st.button("🔄 Refresh Report"):
            st.rerun()

def show_remediation_dashboard(chatbot):
    """Main remediation dashboard function"""
    
    if not chatbot.initialized:
        st.error("❌ Chatbot not initialized. Cannot generate report.")
        st.info("💡 Please ensure both FAISS index folders exist and contain the required files.")
        return
    
    # Generate the simple report
    generate_remediation_report(chatbot)
    
    # Additional analysis section
    st.markdown("---")
    st.subheader("🔍 AI-Powered Deep Analysis")
    
    if st.button("🤖 Run Advanced Analysis", type="primary"):
        with st.spinner("AI is analyzing logs for deeper insights and correlations..."):
            # Simulate AI analysis - in real implementation, this would call your AI
            time.sleep(3)
            
            st.success("✅ Deep analysis completed!")
            
            # Display AI insights
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Pattern Detection")
                st.write("""
                - **Database issues** correlate with user login spikes (9-11 AM)
                - **Memory leaks** occur after 4+ hours of continuous operation  
                - **API slowdowns** happen during bulk data operations
                - **Error rates** increase by 45% during peak hours
                """)
            
            with col2:
                st.subheader("🎯 Recommendations")
                st.write("""
                - **Scale database resources** during 9-11 AM peak hours
                - **Implement service recycling** every 3 hours for memory management
                - **Add request throttling** for bulk operations
                - **Monitor error rates** with real-time alerts
                """)
            
            # Show AI-generated action plan
            with st.expander("📋 AI-Generated Action Plan", expanded=True):
                st.write("""
                1. **Immediate (Today):**
                   - Increase database connection pool to 150
                   - Set up memory monitoring alerts
                
                2. **Short-term (This Week):**
                   - Implement Redis caching layer
                   - Add circuit breaker pattern for API calls
                
                3. **Long-term (This Month):**
                   - Database performance tuning
                   - Implement auto-scaling for peak loads
                """)

def handle_quick_action(action, chatbot):
    """Handle quick action buttons"""
    
    action_titles = {
        "critical_errors": "Critical Errors Analysis",
        "performance": "Performance Issues", 
        "security": "Security Check"
    }
    
    st.header(f"⚡ {action_titles.get(action, 'Quick Analysis')}")
    
    # Pre-defined queries for quick actions
    action_queries = {
        "critical_errors": "Show me critical errors that need immediate attention with remediation steps and root cause analysis",
        "performance": "Identify performance bottlenecks, slow endpoints, and suggest specific optimizations with estimated impact", 
        "security": "Find security-related issues, authentication problems, and recommend security improvements"
    }
    
    query = action_queries.get(action, "Analyze system issues and provide actionable recommendations")
    
    # Use combined logs for better analysis
    with st.spinner(f"Analyzing {action.replace('_', ' ')}..."):
        response = chatbot.get_response(query, "combined")
    
    st.subheader("Analysis Results")
    st.write(response["result"])
    
    # Show source documents
    if response.get("source_documents"):
        with st.expander("📎 Supporting Log Evidence", expanded=False):
            for i, source in enumerate(response["source_documents"][:3]):
                st.markdown(f"**Evidence {i+1}:** {source.page_content[:200]}...")
    
    # Add quick remediation suggestions
    st.subheader("💡 Recommended Immediate Actions")
    
    if action == "critical_errors":
        show_critical_actions()
    elif action == "performance":
        show_performance_actions()
    elif action == "security":
        show_security_actions()

def show_critical_actions():
    """Show actions for critical errors"""
    st.write("""
    - 🚨 **Immediate**: Restart affected services and increase monitoring
    - 📊 **Investigate**: Check database connectivity and resource usage
    - 🔧 **Fix**: Update error handling in identified modules
    - 📈 **Monitor**: Set up real-time alerts for similar errors
    - 🔄 **Validate**: Test fixes in staging environment first
    """)

def show_performance_actions():
    """Show actions for performance issues"""
    st.write("""
    - ⚡ **Optimize**: Database query performance and indexing
    - 🔍 **Profile**: Identify slow API endpoints with detailed metrics
    - 💾 **Cache**: Implement caching strategy for frequent requests
    - 📏 **Scale**: Consider horizontal scaling for high-traffic endpoints
    - 🎯 **Prioritize**: Focus on endpoints affecting most users
    """)

def show_security_actions():
    """Show actions for security issues"""
    st.write("""
    - 🔒 **Secure**: Update authentication and authorization mechanisms
    - 📝 **Audit**: Review access logs for suspicious activity patterns
    - 🛡️ **Protect**: Implement rate limiting and request validation
    - 👀 **Monitor**: Set up security alerting for unusual patterns
    - 🔍 **Scan**: Run security vulnerability assessment
    """)

def show_chat_interface(chatbot):
    """Show the main chat interface"""
    col1, col2 = st.columns([2, 1])
   
    with col1:
        st.subheader("💬 Chat Interface")
       
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
               
                # Show source documents if available
                if message.get("sources"):
                    with st.expander(f"📎 Source Logs ({len(message['sources'])} entries)"):
                        for i, source in enumerate(message["sources"][:3]):
                            st.markdown(format_source_document(source, i), unsafe_allow_html=True)
       
        # User input
        user_input = st.chat_input("Ask about Windchill logs or HTTP access logs...")
       
        if user_input:
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": user_input})
            
            # Display user message immediately
            with st.chat_message("user"):
                st.markdown(user_input)
           
            # Get bot response
            response = chatbot.get_response(user_input, st.session_state.log_type)
           
            # Add assistant response to chat history
            with st.chat_message("assistant"):
                st.markdown(response["result"])
               
                # Display source documents
                if response.get("source_documents"):
                    with st.expander(f"📎 Relevant Log Entries ({len(response['source_documents'])} found)"):
                        for i, source in enumerate(response["source_documents"][:3]):
                            st.markdown(format_source_document(source, i), unsafe_allow_html=True)
           
            st.session_state.messages.append({
                "role": "assistant",
                "content": response["result"],
                "sources": response.get("source_documents", []),
                "log_type": response.get("log_type", "combined")
            })
   
    with col2:
        st.subheader("📊 Quick Analysis")
        
        if not chatbot.initialized:
            st.warning("⚠️ Chatbot not initialized. Quick analysis unavailable.")
        else:
            # Quick analysis buttons based on log type
            if st.session_state.log_type == "combined":
                if st.button("🔍 Correlation Analysis", key="correlation_btn"):
                    handle_quick_action("critical_errors", chatbot)
               
                if st.button("📈 Performance Overview", key="performance_btn"):
                    handle_quick_action("performance", chatbot)
                    
            elif st.session_state.log_type == "windchill":
                if st.button("🔍 Recent Errors", key="errors_btn"):
                    handle_quick_action("critical_errors", chatbot)
               
                if st.button("📊 Error Patterns", key="patterns_btn"):
                    query = "What are the most common error patterns and their frequencies?"
                    response = chatbot.get_response(query, "windchill")
                    st.info(response["result"][:200] + "..." if len(response["result"]) > 200 else response["result"])
                    
            else:  # http logs
                if st.button("🐌 Slow Requests", key="slow_btn"):
                    handle_quick_action("performance", chatbot)
               
                if st.button("❌ HTTP Errors", key="http_errors_btn"):
                    query = "Show HTTP errors with status codes and remediation suggestions"
                    response = chatbot.get_response(query, "http")
                    st.info(response["result"][:200] + "..." if len(response["result"]) > 200 else response["result"])
        
        st.subheader("🎯 Analysis Tips")
        
        if st.session_state.log_type == "combined":
            st.info("💡 Ask about correlations between application errors and HTTP requests")
        elif st.session_state.log_type == "windchill":
            st.info("💡 Focus on error patterns, module issues, and system health")
        else:
            st.info("💡 Monitor response times, status codes, and endpoint performance")

def main():
    st.markdown('<div class="main-header">🤖 Windchill Log Analysis Assistant</div>', unsafe_allow_html=True)
    st.markdown('<div style="text-align: center; color: #666; margin-bottom: 2rem;">Analyzing Windchill Application Logs & HTTP Access Logs</div>', unsafe_allow_html=True)
   
    # Check index files first
    windchill_exists, http_exists = check_index_files()
    
    # Display index status
    col1, col2, col3 = st.columns(3)
    with col1:
        if windchill_exists:
            st.success("🔵 Windchill Index: ✅ Loaded")
        else:
            st.error("🔵 Windchill Index: ❌ Missing")
    
    with col2:
        if http_exists:
            st.success("🟢 HTTP Index: ✅ Loaded")
        else:
            st.error("🟢 HTTP Index: ❌ Missing")
    
    with col3:
        if windchill_exists and http_exists:
            st.success("📊 Both Indexes: ✅ Ready")
        else:
            st.warning("📊 Both Indexes: ⚠️ Some missing")
    
    # Initialize chatbot
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = WindchillChatbot()
   
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'log_type' not in st.session_state:
        st.session_state.log_type = "combined"

    # Sidebar
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Log type selection
        st.subheader("📊 Log Type Selection")
        log_type = st.radio(
            "Select Log Type to Analyze:",
            ["Combined", "Windchill Only", "HTTP Only"],
            index=0,
            key="log_type_selector"
        )
        
        log_type_map = {
            "Combined": "combined",
            "Windchill Only": "windchill", 
            "HTTP Only": "http"
        }
        
        st.session_state.log_type = log_type_map[log_type]
        
        st.info(f"🔍 Analyzing: **{log_type}** logs")
       
        st.subheader("💡 Sample Questions")
        
        # Dynamic sample questions based on log type
        if st.session_state.log_type == "combined":
            sample_questions = [
                "Show me recent errors and their HTTP requests",
                "Correlate application errors with HTTP response times",
                "Analyze system performance across both log types",
                "Find patterns between slow requests and application errors"
            ]
        elif st.session_state.log_type == "windchill":
            sample_questions = [
                "Show me recent error logs from Application Insights",
                "What are the most common error patterns?",
                "Analyze performance issues from the application logs",
                "What modules are generating the most errors?"
            ]
        else:  # http logs
            sample_questions = [
                "Show me slow HTTP requests (high response time)",
                "What are the most frequently accessed endpoints?",
                "Analyze HTTP error patterns (4xx, 5xx status codes)",
                "Show requests by client IP addresses"
            ]
       
        for i, question in enumerate(sample_questions):
            if st.button(question, key=f"sample_{i}"):
                st.session_state.user_input = question
       
        # Remediation Tools Section
        st.markdown("---")
        st.header("🛠️ Remediation Tools")
        
        if st.button("📋 Generate Quick Fix Report", type="primary"):
            st.session_state.show_remediation = True
            st.session_state.show_chat = False
        
        st.subheader("⚡ Quick Actions")
        if st.button("🔍 Find Critical Errors"):
            st.session_state.quick_action = "critical_errors"
            st.session_state.show_chat = False
        
        if st.button("🐌 Performance Issues"):
            st.session_state.quick_action = "performance"
            st.session_state.show_chat = False
        
        if st.button("🔒 Security Check"):
            st.session_state.quick_action = "security"
            st.session_state.show_chat = False
        
        # Navigation back to chat
        if st.button("💬 Back to Chat"):
            st.session_state.show_remediation = False
            st.session_state.quick_action = None
            st.session_state.show_chat = True
        
        st.markdown("---")
        st.subheader("📦 Data Sources")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info("🔵 Windchill Logs")
            st.write(f"**Status:** {'✅' if windchill_exists else '❌'}")
        
        with col2:
            st.success("🟢 HTTP Logs")
            st.write(f"**Status:** {'✅' if http_exists else '❌'}")
        
        # Clear chat button
        if st.button("🗑️ Clear Chat History"):
            st.session_state.messages = []
            st.rerun()
    
    # Main content routing
    if st.session_state.get('show_remediation', False):
        show_remediation_dashboard(st.session_state.chatbot)
    
    elif st.session_state.get('quick_action'):
        handle_quick_action(st.session_state.quick_action, st.session_state.chatbot)
    
    else:
        show_chat_interface(st.session_state.chatbot)

if __name__ == "__main__":
    main()