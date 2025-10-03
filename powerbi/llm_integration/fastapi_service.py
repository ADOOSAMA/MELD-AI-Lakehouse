"""
FastAPI Web Service for Power BI LLM Q&A

Provides REST API interface for LLM Q&A service
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
import uvicorn
from src.mock_llm import MockLLMService

app = FastAPI(
    title="ICU Data Q&A API",
    description="Natural language query interface for ICU data and MELD scores",
    version="1.0.0"
)

# Initialize mock LLM service
llm_processor = MockLLMService()

class QueryRequest(BaseModel):
    query: str
    user_id: Optional[str] = "anonymous"
    session_id: Optional[str] = None

class QueryResponse(BaseModel):
    success: bool
    query: str
    intent: str
    dax_query: str
    result: Optional[List[Dict]]
    explanation: Optional[str]
    execution_time: Optional[str]
    error: Optional[str] = None

@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """Process natural language query"""
    try:
        result = llm_processor.process_query(request.query)
        return QueryResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/suggestions/{intent}")
async def get_suggestions(intent: str):
    """Get query suggestions"""
    suggestions = llm_processor.get_query_suggestions(intent)
    return {"intent": intent, "suggestions": suggestions}

@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "healthy", "service": "ICU Data Q&A API"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
