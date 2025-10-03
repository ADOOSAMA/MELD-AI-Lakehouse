"""
Power BI LLM Q&A Interface

Implements natural language query interface for Power BI and LLM, supports secure DAX query generation and execution
"""

import streamlit as st
import yaml
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd

# Add project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.mock_llm import MockLLMService

class PowerBIQAInterface:
    """Power BI Q&A interface"""
    
    def __init__(self, config_path: str = None):
        # Set correct configuration file path
        if config_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(project_root, "config", "llm_config.yaml")
        
        self.config_path = config_path
        self.llm_processor = LLMQAProcessor(config_path)
        
        # Initialize mock LLM service for testing
        self.mock_llm = MockLLMService()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def run_streamlit_app(self):
        """Run Streamlit application"""
        st.set_page_config(
            page_title="ICU Data Q&A",
            page_icon="🏥",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Main title
        st.title("🏥 ICU Data Intelligence Hub")
        st.markdown("### Natural Language Query Interface for MELD Scores and Clinical Data")
        
        # Sidebar
        self._create_sidebar()
        
        # Main interface
        self._create_main_interface()
        
        # Footer
        self._create_footer()
    
    def _create_sidebar(self):
        """Create sidebar"""
        with st.sidebar:
            st.header("🔧 Configuration")
            
            # Query type selection
            query_type = st.selectbox(
                "Query Type",
                ["Data Query", "Trend Analysis", "Patient Search", "MELD Score", "Risk Assessment"]
            )
            
            # Time range
            time_range = st.selectbox(
                "Time Range",
                ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last Year", "All Time"]
            )
            
            # Data source
            data_source = st.selectbox(
                "Data Source",
                ["All Data", "High Risk Only", "Critical Risk Only", "Recent Data"]
            )
            
            st.divider()
            
            # Query suggestions
            st.header("💡 Query Suggestions")
            suggestions = self._get_query_suggestions(query_type)
            
            for suggestion in suggestions:
                if st.button(suggestion, key=f"suggestion_{suggestion}"):
                    st.session_state.query_input = suggestion
    
    def _create_main_interface(self):
        """Create main interface"""
        # Query input area
        col1, col2 = st.columns([3, 1])
        
        with col1:
            query_input = st.text_input(
                "Ask a question about ICU data:",
                value=st.session_state.get('query_input', ''),
                placeholder="e.g., Show me the average MELD score for high-risk patients",
                key="query_input"
            )
        
        with col2:
            if st.button("🔍 Query", type="primary", use_container_width=True):
                if query_input:
                    self._process_query(query_input)
                else:
                    st.warning("Please enter a query")
        
        # Results display area
        if 'query_result' in st.session_state:
            self._display_query_result(st.session_state.query_result)
        
        # Query history
        if 'query_history' in st.session_state:
            self._display_query_history()
    
    def _create_footer(self):
        """Create footer"""
        st.divider()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**📊 Data Sources**")
            st.markdown("- MELD Scores")
            st.markdown("- Patient Demographics")
            st.markdown("- Lab Results")
        
        with col2:
            st.markdown("**🔒 Security**")
            st.markdown("- Row-level Security")
            st.markdown("- Query Validation")
            st.markdown("- Audit Logging")
        
        with col3:
            st.markdown("**📈 Features**")
            st.markdown("- Natural Language")
            st.markdown("- Real-time Data")
            st.markdown("- Clinical Insights")
    
    def _get_query_suggestions(self, query_type: str) -> List[str]:
        """Get query suggestions"""
        suggestions_map = {
            "Data Query": [
                "Show me the average MELD score",
                "How many high-risk patients do we have?",
                "What is the total number of patients?",
                "Display the current ICU capacity"
            ],
            "Trend Analysis": [
                "How is the MELD score trending?",
                "Show me the 30-day trend",
                "Compare this month to last month",
                "What is the risk trend over time?"
            ],
            "Patient Search": [
                "Find patients with MELD score above 25",
                "Show me critical risk patients",
                "List patients admitted this week",
                "Find patients by risk category"
            ],
            "MELD Score": [
                "Calculate the current MELD score",
                "What factors affect MELD score?",
                "Show MELD score distribution",
                "Compare MELD scores by category"
            ],
            "Risk Assessment": [
                "Which patients are highest risk?",
                "Show risk stratification",
                "How many patients are improving?",
                "What is the risk trend?"
            ]
        }
        
        return suggestions_map.get(query_type, suggestions_map["Data Query"])
    
    def _process_query(self, query: str):
        """Process query"""
        with st.spinner("Processing your query..."):
            try:
                # Use mock LLM service to process query
                result = self.mock_llm.get_mock_result(query)
                
                # Store result to session state
                st.session_state.query_result = result
                
                # Record query history
                if 'query_history' not in st.session_state:
                    st.session_state.query_history = []
                
                st.session_state.query_history.append({
                    'timestamp': datetime.now(),
                    'query': query,
                    'success': result['success'],
                    'intent': result.get('intent', 'unknown')
                })
                
                # Log query audit
                self.logger.info(f"Query processed: {query}")
                
            except Exception as e:
                st.error(f"Error processing query: {e}")
                self.logger.error(f"Query processing error: {e}")
    
    def _display_query_result(self, result: Dict[str, Any]):
        """Display query results"""
        st.subheader("📊 Query Results")
        
        if result['success']:
            # Display query information
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Query Intent", result.get('intent', 'Unknown'))
            
            with col2:
                st.metric("Execution Time", result.get('execution_time', 'N/A'))
            
            with col3:
                st.metric("Status", "✅ Success")
            
            # Display DAX query
            with st.expander("🔍 Generated DAX Query", expanded=False):
                st.code(result.get('dax_query', ''), language='sql')
            
            # Display data results
            if result.get('result'):
                st.subheader("📈 Data Results")
                
                # Convert to DataFrame for display
                if isinstance(result['result'], list):
                    df = pd.DataFrame(result['result'])
                    st.dataframe(df, use_container_width=True)
                    
                    # If there is numeric data, display charts
                    numeric_columns = df.select_dtypes(include=['number']).columns
                    if len(numeric_columns) > 0:
                        st.subheader("📊 Visualization")
                        
                        chart_type = st.selectbox(
                            "Chart Type",
                            ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot"]
                        )
                        
                        if chart_type == "Bar Chart":
                            st.bar_chart(df.set_index(df.columns[0]) if len(df.columns) > 1 else df)
                        elif chart_type == "Line Chart":
                            st.line_chart(df.set_index(df.columns[0]) if len(df.columns) > 1 else df)
                        elif chart_type == "Pie Chart":
                            if len(df.columns) >= 2:
                                st.pyplot(self._create_pie_chart(df))
                        elif chart_type == "Scatter Plot":
                            if len(numeric_columns) >= 2:
                                st.scatter_chart(df[numeric_columns])
                else:
                    st.json(result['result'])
            
            # Display explanation
            if result.get('explanation'):
                st.subheader("💡 Clinical Interpretation")
                st.info(result['explanation'])
        
        else:
            # Display error information
            st.error(f"❌ Query Failed: {result.get('error', 'Unknown error')}")
            
            # Provide suggestions
            st.subheader("💡 Suggestions")
            st.markdown("- Try rephrasing your question")
            st.markdown("- Check if the data you're asking about is available")
            st.markdown("- Use simpler language")
            st.markdown("- Try one of the suggested queries")
    
    def _display_query_history(self):
        """Display query history"""
        if st.session_state.query_history:
            st.subheader("📝 Query History")
            
            # Create history DataFrame
            history_df = pd.DataFrame(st.session_state.query_history)
            history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
            history_df = history_df.sort_values('timestamp', ascending=False)
            
            # Display last 5 records
            recent_history = history_df.head(5)
            
            for _, record in recent_history.iterrows():
                with st.expander(f"{record['timestamp'].strftime('%H:%M:%S')} - {record['query'][:50]}..."):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Query:** {record['query']}")
                        st.write(f"**Intent:** {record['intent']}")
                    
                    with col2:
                        status = "✅ Success" if record['success'] else "❌ Failed"
                        st.write(f"**Status:** {status}")
    
    def _create_pie_chart(self, df: pd.DataFrame):
        """Create pie chart"""
        import matplotlib.pyplot as plt
        import numpy as np
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        if len(df.columns) >= 2:
            # Ensure data is numeric type
            values = df.iloc[:, 1]
            labels = df.iloc[:, 0]
            
            # Try to convert to numeric type
            try:
                values = pd.to_numeric(values, errors='coerce')
                # Remove NaN values
                valid_mask = ~values.isna()
                values = values[valid_mask]
                labels = labels[valid_mask]
                
                if len(values) > 0:
                    ax.pie(values, labels=labels, autopct='%1.1f%%')
                    ax.set_title('Data Distribution')
                else:
                    ax.text(0.5, 0.5, 'No valid data for pie chart', 
                           ha='center', va='center', transform=ax.transAxes)
            except Exception as e:
                ax.text(0.5, 0.5, f'Error creating pie chart: {str(e)}', 
                       ha='center', va='center', transform=ax.transAxes)
        
        return fig

def main():
    """Main function"""
    # Create Q&A interface
    qa_interface = PowerBIQAInterface()
    
    # Run Streamlit application
    qa_interface.run_streamlit_app()

if __name__ == "__main__":
    main()

