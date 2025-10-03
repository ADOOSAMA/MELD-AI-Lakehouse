#!/usr/bin/env python3
"""
MELD Score Analysis System with NLP Query Interface
Description: A comprehensive medical data analysis system for ICU patient monitoring
             with natural language processing capabilities for clinical decision support.
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import os
import sys
import re
from typing import Dict, List, Any, Optional

# Set page configuration
st.set_page_config(
    page_title="MELD Score Analysis System",
    layout="wide"
)

class NLPQueryProcessor:
    """
    Natural Language Query Processor for MELD Score Analysis
    
    This class handles natural language queries about medical data.
    """
    
    def __init__(self, data: pd.DataFrame):
        """Initialize with medical data"""
        self.data = data
        self.query_patterns = {
            'average_score': [
                r'average.*meld.*score',
                r'mean.*meld.*score',
                r'what.*is.*the.*average.*meld',
                r'calculate.*average.*meld'
            ],
            'high_risk_patients': [
                r'high.*risk.*patients',
                r'patients.*with.*high.*risk',
                r'how.*many.*high.*risk',
                r'count.*high.*risk'
            ],
            'score_distribution': [
                r'score.*distribution',
                r'meld.*score.*range',
                r'what.*is.*the.*range.*of.*scores',
                r'show.*score.*distribution'
            ],
            'risk_breakdown': [
                r'risk.*breakdown',
                r'risk.*categories',
                r'how.*many.*in.*each.*risk',
                r'breakdown.*by.*risk'
            ],
            'statistics': [
                r'statistics',
                r'summary.*statistics',
                r'statistical.*summary',
                r'give.*me.*statistics'
            ],
            'trend_analysis': [
                r'trend.*analysis',
                r'score.*trends',
                r'how.*are.*scores.*changing',
                r'time.*series.*analysis'
            ]
        }
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """Process natural language query and return results"""
        query_lower = query.lower().strip()
        intent = self._classify_intent(query_lower)
        
        if intent == 'average_score':
            return self._get_average_score()
        elif intent == 'high_risk_patients':
            return self._get_high_risk_patients()
        elif intent == 'score_distribution':
            return self._get_score_distribution()
        elif intent == 'risk_breakdown':
            return self._get_risk_breakdown()
        elif intent == 'statistics':
            return self._get_statistics()
        elif intent == 'trend_analysis':
            return self._get_trend_analysis()
        else:
            return self._get_general_info()
    
    def _classify_intent(self, query: str) -> str:
        """Classify the intent of the query"""
        for intent, patterns in self.query_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query):
                    return intent
        return 'general'
    
    def _get_average_score(self) -> Dict[str, Any]:
        """Calculate and return average MELD score"""
        avg_score = self.data['meld_score'].mean()
        
        return {
            'success': True,
            'result': f"Average MELD Score: {avg_score:.2f}",
            'data': {'average_score': avg_score},
            'visualization': 'metric'
        }
    
    def _get_high_risk_patients(self) -> Dict[str, Any]:
        """Get count of high risk patients"""
        high_risk = len(self.data[self.data['risk_category'] == 'High Risk'])
        total = len(self.data)
        percentage = (high_risk / total) * 100
        
        return {
            'success': True,
            'result': f"High Risk Patients: {high_risk} out of {total} ({percentage:.1f}%)",
            'data': {'high_risk_count': high_risk, 'total_count': total, 'percentage': percentage},
            'visualization': 'metric'
        }
    
    def _get_score_distribution(self) -> Dict[str, Any]:
        """Get MELD score distribution"""
        min_score = self.data['meld_score'].min()
        max_score = self.data['meld_score'].max()
        median_score = self.data['meld_score'].median()
        
        return {
            'success': True,
            'result': f"Score Range: {min_score:.1f} - {max_score:.1f}, Median: {median_score:.1f}",
            'data': {'min': min_score, 'max': max_score, 'median': median_score},
            'visualization': 'histogram'
        }
    
    def _get_risk_breakdown(self) -> Dict[str, Any]:
        """Get risk category breakdown"""
        risk_counts = self.data['risk_category'].value_counts()
        breakdown = []
        for risk, count in risk_counts.items():
            percentage = (count / len(self.data)) * 100
            breakdown.append(f"{risk}: {count} ({percentage:.1f}%)")
        
        return {
            'success': True,
            'result': "Risk Category Breakdown:\n" + "\n".join(breakdown),
            'data': risk_counts.to_dict(),
            'visualization': 'pie_chart'
        }
    
    def _get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        stats = {
            'total_patients': len(self.data),
            'average_score': self.data['meld_score'].mean(),
            'median_score': self.data['meld_score'].median(),
            'std_score': self.data['meld_score'].std(),
            'min_score': self.data['meld_score'].min(),
            'max_score': self.data['meld_score'].max()
        }
        
        result = f"""MELD Score Statistics:
• Total Patients: {stats['total_patients']}
• Average Score: {stats['average_score']:.2f}
• Median Score: {stats['median_score']:.2f}
• Standard Deviation: {stats['std_score']:.2f}
• Score Range: {stats['min_score']:.1f} - {stats['max_score']:.1f}"""
        
        return {
            'success': True,
            'result': result,
            'data': stats,
            'visualization': 'table'
        }
    
    def _get_trend_analysis(self) -> Dict[str, Any]:
        """Get trend analysis (simplified)"""
        # Group by date and calculate average scores
        daily_avg = self.data.groupby(self.data['score_date'].dt.date)['meld_score'].mean()
        
        if len(daily_avg) > 1:
            trend = "increasing" if daily_avg.iloc[-1] > daily_avg.iloc[0] else "decreasing"
            change = daily_avg.iloc[-1] - daily_avg.iloc[0]
        else:
            trend = "stable"
            change = 0
        
        return {
            'success': True,
            'result': f"Score Trend: {trend.title()} (Change: {change:+.2f})",
            'data': {'trend': trend, 'change': change, 'daily_averages': daily_avg.to_dict()},
            'visualization': 'line_chart'
        }
    
    def _get_general_info(self) -> Dict[str, Any]:
        """Get general information about the dataset"""
        return {
            'success': True,
            'result': f"Dataset contains {len(self.data)} patients with MELD scores ranging from {self.data['meld_score'].min():.1f} to {self.data['meld_score'].max():.1f}",
            'data': {'total_patients': len(self.data)},
            'visualization': 'info'
        }

class MELDScoreAnalyzer:
    """
    MELD Score Analyzer - Main application class
    
    Central orchestrator for the entire system.
    It handles data loading, NLP processing, and user interface management.
    """
    
    def __init__(self):
        """Initialize the analyzer with lazy loading for better performance"""
        self.data = None
        self.nlp_processor = None
        self.load_data()
    
    def load_data(self):
        """Load MELD score data"""
        try:
            # Try to load data file
            data_path = "data/meld_scores.csv"
            if os.path.exists(data_path):
                self.data = pd.read_csv(data_path)
                st.success(f"Successfully loaded data: {len(self.data)} records")
            else:
                # Create sample data
                self.create_sample_data()
                st.info("Using sample data")
        except Exception as e:
            st.error(f"Data loading failed: {e}")
            self.create_sample_data()
    
    def create_sample_data(self):
        """
        Create sample MELD score data for demonstration
        
        Generated synthetic data to represent realistic ICU scenarios.
        The parameters are based on clinical literature for MELD score distributions.
        """
        # Use seed 42 for reproducibility
        np.random.seed(42)
        n_patients = 100  # Good balance for demo purposes
        
        data = {
            'patient_id': [f'P{10000 + i}' for i in range(n_patients)],
            'meld_score': np.random.normal(15, 8, n_patients).round(1),
            'bilirubin': np.random.normal(2.0, 1.5, n_patients).round(2),
            'inr': np.random.normal(1.2, 0.3, n_patients).round(2),
            'creatinine': np.random.normal(1.5, 0.8, n_patients).round(2),
            'sodium': np.random.normal(140, 5, n_patients).round(1),
            'score_date': pd.date_range('2024-01-01', periods=n_patients, freq='D')
        }
        
        self.data = pd.DataFrame(data)
        
        # Ensure MELD scores are in reasonable range
        self.data['meld_score'] = np.clip(self.data['meld_score'], 6, 40)
        
        # Add risk classification
        self.data['risk_category'] = self.data['meld_score'].apply(self.classify_risk)
        
        # Initialize NLP processor
        self.nlp_processor = NLPQueryProcessor(self.data)
    
    def classify_risk(self, score):
        """
        Classify risk level based on MELD score
        
        Using standard clinical thresholds:
        - High Risk: ≥25 (immediate transplant consideration)
        - Medium Risk: 15-24 (monitoring required)
        - Low Risk: <15 (routine follow-up)
        """
        if score >= 25:
            return "High Risk"
        elif score >= 15:
            return "Medium Risk"
        else:
            return "Low Risk"
    
    def run_app(self):
        """Run application"""
        st.title("MELD Score Analysis System")
        st.markdown("---")
        
        # NLP Query Interface
        self._create_nlp_interface()
        
        st.markdown("---")
        
        # Sidebar
        with st.sidebar:
            st.header("Analysis Options")
            
            # Risk level filter
            risk_options = ["All"] + list(self.data['risk_category'].unique())
            selected_risk = st.selectbox("Select Risk Level", risk_options)
            
            # Date range filter
            min_date = self.data['score_date'].min()
            max_date = self.data['score_date'].max()
            date_range = st.date_input("Select Date Range", value=(min_date, max_date))
            
            # Update data
            filtered_data = self.filter_data(selected_risk, date_range)
        
        # Main interface
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Patients", len(filtered_data))
        
        with col2:
            avg_score = filtered_data['meld_score'].mean()
            st.metric("Average MELD Score", f"{avg_score:.1f}")
        
        with col3:
            high_risk = len(filtered_data[filtered_data['risk_category'] == 'High Risk'])
            st.metric("High Risk Patients", high_risk)
        
        with col4:
            max_score = filtered_data['meld_score'].max()
            st.metric("Highest MELD Score", f"{max_score:.1f}")
        
        st.markdown("---")
        
        # Charts area
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("MELD Score Distribution")
            self.plot_score_distribution(filtered_data)
        
        with col2:
            st.subheader("Risk Level Distribution")
            self.plot_risk_distribution(filtered_data)
        
        # Detailed data table
        st.subheader("Patient Details")
        st.dataframe(filtered_data, use_container_width=True)
        
        # Statistics summary
        st.subheader("Statistics Summary")
        self.show_statistics(filtered_data)
    
    def _create_nlp_interface(self):
        """
        Create NLP query interface
        
        Designed to be intuitive for medical professionals
        who need quick insights without learning complex query syntax.
        """
        st.subheader("Natural Language Query")
        st.markdown("Ask questions about the MELD score data in natural language!")
        st.markdown("*This feature makes data analysis more accessible to clinicians.*")
        
        # Example queries
        with st.expander("Example Queries"):
            st.markdown("""
            **Try these example queries:**
            - "What is the average MELD score?"
            - "How many high risk patients are there?"
            - "Show me the score distribution"
            - "Give me statistics"
            - "What is the risk breakdown?"
            - "How are the scores trending?"
            """)
        
        # Query input
        col1, col2 = st.columns([3, 1])
        
        with col1:
            query = st.text_input(
                "Ask a question:",
                placeholder="e.g., What is the average MELD score?",
                key="nlp_query"
            )
        
        with col2:
            if st.button("Query", type="primary"):
                if query:
                    self._process_nlp_query(query)
                else:
                    st.warning("Please enter a query")
        
        # Display query history
        if 'query_history' not in st.session_state:
            st.session_state.query_history = []
        
        if st.session_state.query_history:
            with st.expander("Query History"):
                for i, (q, r) in enumerate(reversed(st.session_state.query_history[-5:])):
                    st.markdown(f"**Q{i+1}:** {q}")
                    st.markdown(f"**A:** {r}")
                    st.markdown("---")
    
    def _process_nlp_query(self, query: str):
        """Process NLP query and display results"""
        try:
            with st.spinner("Processing your query..."):
                result = self.nlp_processor.process_query(query)
            
            if result['success']:
                st.success("Query processed successfully!")
                
                # Display result
                st.markdown("**Answer:**")
                st.write(result['result'])
                
                # Display visualization if applicable
                if result.get('visualization') == 'metric':
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Result", result['data'].get('average_score', result['data'].get('high_risk_count', 'N/A')))
                
                elif result.get('visualization') == 'pie_chart':
                    self._create_risk_pie_chart(result['data'])
                
                elif result.get('visualization') == 'histogram':
                    self._create_score_histogram()
                
                elif result.get('visualization') == 'line_chart':
                    self._create_trend_chart(result['data'])
                
                # Add to history
                st.session_state.query_history.append((query, result['result']))
                
            else:
                st.error("Failed to process query")
                    
        except Exception as e:
            st.error(f"Error processing query: {str(e)}")
    
    def _create_risk_pie_chart(self, data: Dict):
        """Create risk category pie chart"""
        fig, ax = plt.subplots(figsize=(8, 6))
        
        labels = list(data.keys())
        sizes = list(data.values())
        colors = ['lightgreen', 'orange', 'red']
        
        ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors[:len(labels)], startangle=90)
        ax.set_title('Risk Category Distribution')
        
        st.pyplot(fig)
    
    def _create_score_histogram(self):
        """Create MELD score histogram"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        ax.hist(self.data['meld_score'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        ax.axvline(self.data['meld_score'].mean(), color='red', linestyle='--', 
                  label=f'Mean: {self.data["meld_score"].mean():.1f}')
        ax.set_xlabel('MELD Score')
        ax.set_ylabel('Number of Patients')
        ax.set_title('MELD Score Distribution')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        st.pyplot(fig)
    
    def _create_trend_chart(self, data: Dict):
        """Create trend analysis chart"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        daily_avg = data.get('daily_averages', {})
        if daily_avg:
            dates = list(daily_avg.keys())
            scores = list(daily_avg.values())
            
            ax.plot(dates, scores, marker='o', linewidth=2, markersize=6)
            ax.set_xlabel('Date')
            ax.set_ylabel('Average MELD Score')
            ax.set_title('MELD Score Trend Over Time')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            st.pyplot(fig)
    
    def filter_data(self, risk_level, date_range):
        """Filter data"""
        filtered = self.data.copy()
        
        # Risk level filter
        if risk_level != "All":
            filtered = filtered[filtered['risk_category'] == risk_level]
        
        # Date range filter
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered = filtered[
                (filtered['score_date'] >= pd.Timestamp(start_date)) &
                (filtered['score_date'] <= pd.Timestamp(end_date))
            ]
        
        return filtered
    
    def plot_score_distribution(self, data):
        """Plot MELD score distribution"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Histogram
        ax.hist(data['meld_score'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        ax.axvline(data['meld_score'].mean(), color='red', linestyle='--', 
                  label=f'Mean: {data["meld_score"].mean():.1f}')
        ax.set_xlabel('MELD Score')
        ax.set_ylabel('Number of Patients')
        ax.set_title('MELD Score Distribution')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        st.pyplot(fig)
    
    def plot_risk_distribution(self, data):
        """Plot risk level distribution pie chart"""
        fig, ax = plt.subplots(figsize=(8, 8))
        
        risk_counts = data['risk_category'].value_counts()
        colors = ['lightgreen', 'orange', 'red']
        
        ax.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%', 
               colors=colors, startangle=90)
        ax.set_title('Risk Level Distribution')
        
        st.pyplot(fig)
    
    def show_statistics(self, data):
        """Show statistics summary"""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**MELD Score Statistics**")
            st.write(f"• Min: {data['meld_score'].min():.1f}")
            st.write(f"• Max: {data['meld_score'].max():.1f}")
            st.write(f"• Median: {data['meld_score'].median():.1f}")
            st.write(f"• Std Dev: {data['meld_score'].std():.1f}")
        
        with col2:
            st.write("**Biochemical Parameters**")
            st.write(f"• Bilirubin: {data['bilirubin'].mean():.2f} ± {data['bilirubin'].std():.2f}")
            st.write(f"• INR: {data['inr'].mean():.2f} ± {data['inr'].std():.2f}")
            st.write(f"• Creatinine: {data['creatinine'].mean():.2f} ± {data['creatinine'].std():.2f}")
            st.write(f"• Sodium: {data['sodium'].mean():.1f} ± {data['sodium'].std():.1f}")
        
        with col3:
            st.write("**Risk Distribution**")
            risk_dist = data['risk_category'].value_counts()
            for risk, count in risk_dist.items():
                percentage = (count / len(data)) * 100
                st.write(f"• {risk}: {count} ({percentage:.1f}%)")

def main():
    """
    Main function - Entry point for the application
    
    Simple entry point that initializes
    the analyzer and runs the Streamlit interface.
    """
    # Initialize the analyzer
    analyzer = MELDScoreAnalyzer()
    
    # Run the main application interface
    analyzer.run_app()

if __name__ == "__main__":
    # Main execution
    main()
