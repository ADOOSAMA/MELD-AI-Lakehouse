"""
Mock LLM Service - For testing and demonstration
"""

import random
from typing import List, Dict, Any

class MockLLMService:
    """Mock LLM service"""
    
    def __init__(self):
        self.mock_responses = {
            "intent_classification": {
                "Show average MELD score": "data_query",
                "How is MELD score trending": "trend_analysis", 
                "Find high-risk patients": "patient_search",
                "Calculate MELD score": "meld_score",
                "Which patients have highest risk": "risk_assessment"
            },
            "dax_generation": {
                "Show average MELD score": "AVERAGE(meld_scores[meld_score])",
                "How is MELD score trending": "CALCULATE(AVERAGE(meld_scores[meld_score]), DATESINPERIOD(meld_scores[charttime], TODAY(), -30, DAY))",
                "Find high-risk patients": "CALCULATE(COUNTROWS(meld_scores), meld_scores[risk_category] = \"High\")",
                "Calculate MELD score": "COUNTROWS(meld_scores)",
                "Which patients have highest risk": "CALCULATE(COUNTROWS(meld_scores), meld_scores[risk_category] = \"Critical\")"
            },
            "explanations": {
                "Show average MELD score": "This query shows the average MELD score for all patients. MELD score is an important indicator for assessing the prognosis of liver disease patients, with normal range typically between 6-40.",
                "How is MELD score trending": "This query analyzes the trend of MELD score changes over the past 30 days, helping to understand the direction of patient condition development.",
                "Find high-risk patients": "This query counts the total number of high-risk patients, where high risk typically refers to patients with MELD score over 25.",
                "Calculate MELD score": "This query calculates the total number of MELD scores, which are calculated based on bilirubin, INR and creatinine values.",
                "Which patients have highest risk": "This query counts the number of critical patients (highest MELD scores), who need priority attention."
            }
        }
    
    def classify_intent(self, query: str) -> str:
        """Mock query intent classification"""
        query_lower = query.lower()
        
        # Keyword matching
        if any(word in query_lower for word in ["average", "mean"]):
            return "data_query"
        elif any(word in query_lower for word in ["trend", "change"]):
            return "trend_analysis"
        elif any(word in query_lower for word in ["find", "search"]):
            return "patient_search"
        elif any(word in query_lower for word in ["calculate", "meld"]):
            return "meld_score"
        elif any(word in query_lower for word in ["risk", "highest"]):
            return "risk_assessment"
        else:
            return "data_query"
    
    def generate_dax(self, query: str, intent: str) -> str:
        """Mock DAX query generation"""
        query_lower = query.lower()
        
        # Generate DAX based on query content
        if "average" in query_lower:
            if "meld" in query_lower:
                return "AVERAGE(meld_scores[meld_score])"
            else:
                return "AVERAGE(meld_scores[meld_score])"
        elif "count" in query_lower:
            if "patient" in query_lower:
                return "COUNTROWS(patients)"
            else:
                return "COUNTROWS(meld_scores)"
        elif "high risk" in query_lower:
            return "CALCULATE(COUNTROWS(meld_scores), meld_scores[risk_category] = \"High\")"
        elif "trend" in query_lower:
            return "CALCULATE(AVERAGE(meld_scores[meld_score]), DATESINPERIOD(meld_scores[charttime], TODAY(), -30, DAY))"
        else:
            return "COUNTROWS(meld_scores)"
    
    def generate_explanation(self, query: str, dax_query: str, result: Dict[str, Any]) -> str:
        """Mock explanation generation"""
        query_lower = query.lower()
        
        if "average" in query_lower:
            return "This query calculates the average MELD score for all patients. MELD score is an important indicator for assessing the prognosis of liver disease patients, with normal range typically between 6-40."
        elif "count" in query_lower:
            return "This query counts the total number of patients, helping to understand ICU capacity and patient scale."
        elif "high risk" in query_lower:
            return "This query counts the number of high-risk patients, who typically require closer monitoring and more aggressive treatment."
        elif "trend" in query_lower:
            return "This query analyzes the temporal trend of MELD scores, helping to understand the pattern of patient condition changes."
        else:
            return "This query provides an overview of medical data, helping the medical team understand the current patient status."
    
    def get_mock_result(self, dax_query: str) -> Dict[str, Any]:
        """Generate mock query results"""
        # Generate corresponding mock results based on DAX query
        if "AVERAGE" in dax_query:
            return {
                "success": True,
                "data": [
                    {"value": 18.5, "category": "Average MELD Score"},
                    {"value": 22.3, "category": "Average Bilirubin"},
                    {"value": 1.8, "category": "Average INR"}
                ],
                "query": dax_query,
                "execution_time": "0.05s"
            }
        elif "COUNTROWS" in dax_query:
            return {
                "success": True,
                "data": [
                    {"value": 150, "category": "Total Patients"},
                    {"value": 89, "category": "Total MELD Records"},
                    {"value": 45, "category": "Total Lab Results"}
                ],
                "query": dax_query,
                "execution_time": "0.03s"
            }
        elif "CALCULATE" in dax_query and "High" in dax_query:
            return {
                "success": True,
                "data": [
                    {"value": 25, "category": "High Risk Patients"},
                    {"value": 12, "category": "Critical Risk Patients"},
                    {"value": 38, "category": "Medium Risk Patients"}
                ],
                "query": dax_query,
                "execution_time": "0.04s"
            }
        else:
            return {
                "success": True,
                "data": [
                    {"value": 150, "category": "Total Records"},
                    {"value": 18.5, "category": "Average Score"},
                    {"value": 25, "category": "High Risk Count"}
                ],
                "query": dax_query,
                "execution_time": "0.05s"
            }
