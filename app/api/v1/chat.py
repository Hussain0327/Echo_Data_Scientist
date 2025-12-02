"""
Chat API - Conversational interface with Echo, the data consultant.

Endpoints:
- POST /chat - Send a message to Echo
- POST /chat/load-data - Load data into a chat session
- GET /chat/history/{session_id} - Get conversation history
- DELETE /chat/session/{session_id} - Clear a session
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd
import io
import uuid

from app.services.llm.conversation import (
    get_conversation_service,
    ChatResponse,
    Message,
)
from app.services.llm.context_builder import DataContextBuilder
from app.services.metrics.registry import create_metrics_engine
from app.services.data_autofixer import auto_fix_dataframe


router = APIRouter()


class ChatRequest(BaseModel):
    """Request to send a message to Echo."""
    message: str
    session_id: Optional[str] = None


class ChatMessageResponse(BaseModel):
    """Response from Echo."""
    response: str
    session_id: str
    timestamp: datetime


class SessionHistoryResponse(BaseModel):
    """Conversation history for a session."""
    session_id: str
    messages: List[Dict[str, Any]]
    data_loaded: bool


class DataLoadResponse(BaseModel):
    """Response after loading data into a session."""
    session_id: str
    message: str
    rows: int
    columns: List[str]
    metrics_calculated: int


@router.post("", response_model=ChatMessageResponse)
async def chat(request: ChatRequest):
    """
    Send a message to Echo and get a response.

    If no session_id is provided, a new session will be created.
    """
    service = get_conversation_service()

    # Generate session ID if not provided
    session_id = request.session_id or str(uuid.uuid4())

    try:
        response = await service.chat(
            session_id=session_id,
            user_message=request.message
        )

        return ChatMessageResponse(
            response=response.message,
            session_id=session_id,
            timestamp=response.timestamp
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Chat service error: {str(e)}"
        )


@router.post("/with-data", response_model=ChatMessageResponse)
async def chat_with_data(
    message: str = Query(..., description="Your message to Echo"),
    session_id: Optional[str] = Query(None, description="Session ID (optional)"),
    file: UploadFile = File(..., description="CSV file with your data"),
    calculate_metrics: bool = Query(True, description="Auto-calculate metrics"),
):
    """
    Send a message to Echo along with data file.

    This loads the data into the session context so Echo can analyze it.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV")

    content = await file.read()
    if not content or content.strip() == b'':
        raise HTTPException(status_code=400, detail="File is empty")

    try:
        df = pd.read_csv(io.BytesIO(content))
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="File is empty or invalid")

    if df.empty:
        raise HTTPException(status_code=400, detail="File is empty")

    # Auto-fix data quality issues before analysis
    fix_result = auto_fix_dataframe(df)
    df = fix_result.df

    service = get_conversation_service()
    session_id = session_id or str(uuid.uuid4())

    # Build data context (using cleaned data)
    data_summary, metrics_summary = DataContextBuilder.build_full_context(
        df=df,
        source_name=file.filename
    )

    # Add note about auto-fixes if any were applied
    if fix_result.was_modified:
        fix_note = f"\n\n**Note:** Data was automatically cleaned ({fix_result.total_fixes} fixes applied)."
        data_summary += fix_note

    # Calculate metrics if requested
    if calculate_metrics:
        try:
            engine = create_metrics_engine(df)
            calculated = engine.calculate_all()
            metrics_dict = {r.metric_name: r.model_dump() for r in calculated}
            metrics_summary = DataContextBuilder.build_metrics_summary(metrics_dict)
        except Exception:
            # If metrics fail, continue without them
            pass

    try:
        response = await service.chat(
            session_id=session_id,
            user_message=message,
            data_summary=data_summary,
            metrics_summary=metrics_summary
        )

        return ChatMessageResponse(
            response=response.message,
            session_id=session_id,
            timestamp=response.timestamp
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Chat service error: {str(e)}"
        )


@router.post("/load-data", response_model=DataLoadResponse)
async def load_data_to_session(
    session_id: str = Query(..., description="Session ID to load data into"),
    file: UploadFile = File(..., description="CSV file with your data"),
    calculate_metrics: bool = Query(True, description="Auto-calculate metrics"),
):
    """
    Load data into an existing chat session.

    This updates the session context so Echo can reference the data in subsequent messages.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV")

    content = await file.read()
    if not content or content.strip() == b'':
        raise HTTPException(status_code=400, detail="File is empty")

    try:
        df = pd.read_csv(io.BytesIO(content))
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="File is empty or invalid")

    if df.empty:
        raise HTTPException(status_code=400, detail="File is empty")

    # Auto-fix data quality issues before analysis
    fix_result = auto_fix_dataframe(df)
    df = fix_result.df

    service = get_conversation_service()

    # Build data context (using cleaned data)
    data_summary, _ = DataContextBuilder.build_full_context(
        df=df,
        source_name=file.filename
    )

    # Add note about auto-fixes if any were applied
    if fix_result.was_modified:
        fix_note = f"\n\n**Note:** Data was automatically cleaned ({fix_result.total_fixes} fixes applied)."
        data_summary += fix_note

    metrics_calculated = 0
    metrics_summary = ""

    # Calculate metrics if requested
    if calculate_metrics:
        try:
            engine = create_metrics_engine(df)
            calculated = engine.calculate_all()
            metrics_dict = {r.metric_name: r.model_dump() for r in calculated}
            metrics_summary = DataContextBuilder.build_metrics_summary(metrics_dict)
            metrics_calculated = len(calculated)
        except Exception:
            pass

    # Update session context
    service.update_data_context(
        session_id=session_id,
        data_summary=data_summary,
        metrics_summary=metrics_summary
    )

    return DataLoadResponse(
        session_id=session_id,
        message=f"Data loaded successfully. Echo now has context about your {file.filename}.",
        rows=len(df),
        columns=list(df.columns),
        metrics_calculated=metrics_calculated
    )


@router.get("/history/{session_id}", response_model=SessionHistoryResponse)
async def get_history(session_id: str):
    """Get the conversation history for a session."""
    service = get_conversation_service()
    session = service._sessions.get(session_id)

    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    messages = [
        {
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None
        }
        for msg in session.messages
    ]

    return SessionHistoryResponse(
        session_id=session_id,
        messages=messages,
        data_loaded=bool(session.data_summary)
    )


@router.delete("/session/{session_id}")
async def clear_session(session_id: str):
    """Clear a conversation session."""
    service = get_conversation_service()

    if service.clear_session(session_id):
        return {"message": f"Session {session_id} cleared successfully"}
    else:
        raise HTTPException(status_code=404, detail="Session not found")


@router.get("/sessions")
async def list_sessions():
    """List all active sessions (for debugging/admin)."""
    service = get_conversation_service()

    sessions = []
    for session_id, session in service._sessions.items():
        sessions.append({
            "session_id": session_id,
            "message_count": len(session.messages),
            "has_data": bool(session.data_summary),
            "has_metrics": bool(session.metrics_summary)
        })

    return {"sessions": sessions, "total": len(sessions)}
