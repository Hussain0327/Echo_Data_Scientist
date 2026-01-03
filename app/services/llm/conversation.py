from datetime import datetime
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI
from pydantic import BaseModel

from app.config import get_settings
from app.services.llm.prompts.consultant import build_system_prompt


class Message(BaseModel):
    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ConversationContext(BaseModel):
    session_id: str
    messages: List[Message] = []
    data_summary: Optional[str] = None
    metrics_summary: Optional[str] = None
    metadata: Dict[str, Any] = {}


class ChatResponse(BaseModel):
    message: str
    session_id: str
    timestamp: datetime = None
    tokens_used: Optional[int] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ConversationService:
    def __init__(self):
        settings = get_settings()

        # DeepSeek uses OpenAI-compatible API
        self.client = AsyncOpenAI(
            api_key=settings.DEEPSEEK_API_KEY or settings.OPENAI_API_KEY,
            base_url=settings.DEEPSEEK_BASE_URL if settings.DEEPSEEK_API_KEY else None,
        )
        self.model = settings.DEEPSEEK_MODEL if settings.DEEPSEEK_API_KEY else "gpt-4-turbo-preview"

        # In-memory session storage (replace with Redis for production)
        self._sessions: Dict[str, ConversationContext] = {}

    def get_or_create_session(self, session_id: str) -> ConversationContext:
        if session_id not in self._sessions:
            self._sessions[session_id] = ConversationContext(session_id=session_id)
        return self._sessions[session_id]

    def update_data_context(
        self,
        session_id: str,
        data_summary: Optional[str] = None,
        metrics_summary: Optional[str] = None,
    ) -> None:
        session = self.get_or_create_session(session_id)
        if data_summary is not None:
            session.data_summary = data_summary
        if metrics_summary is not None:
            session.metrics_summary = metrics_summary

    def _format_conversation_history(self, messages: List[Message], max_messages: int = 10) -> str:
        if not messages:
            return "This is the start of the conversation."

        # Take last N messages to avoid token overflow
        recent = messages[-max_messages:]
        formatted = []
        for msg in recent:
            role = "User" if msg.role == "user" else "Echo"
            formatted.append(f"{role}: {msg.content}")

        return "\n".join(formatted)

    def _build_messages(
        self, session: ConversationContext, user_message: str
    ) -> List[Dict[str, str]]:
        # Build system prompt with context
        conversation_history = self._format_conversation_history(session.messages)
        system_prompt = build_system_prompt(
            data_summary=session.data_summary,
            metrics_summary=session.metrics_summary,
            conversation_history=conversation_history,
        )

        messages = [{"role": "system", "content": system_prompt}]

        # Add conversation history as separate messages for better context
        for msg in session.messages[-10:]:  # Last 10 messages
            messages.append({"role": msg.role, "content": msg.content})

        # Add current user message
        messages.append({"role": "user", "content": user_message})

        return messages

    async def chat(
        self,
        session_id: str,
        user_message: str,
        data_summary: Optional[str] = None,
        metrics_summary: Optional[str] = None,
    ) -> ChatResponse:
        # Get or create session
        session = self.get_or_create_session(session_id)

        # Update context if provided
        if data_summary:
            session.data_summary = data_summary
        if metrics_summary:
            session.metrics_summary = metrics_summary

        # Build messages for API
        messages = self._build_messages(session, user_message)

        # Call DeepSeek/OpenAI
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=1000,
            temperature=0.7,
        )

        assistant_message = response.choices[0].message.content
        tokens_used = response.usage.total_tokens if response.usage else None

        # Store messages in history
        session.messages.append(Message(role="user", content=user_message))
        session.messages.append(Message(role="assistant", content=assistant_message))

        return ChatResponse(
            message=assistant_message, session_id=session_id, tokens_used=tokens_used
        )

    def clear_session(self, session_id: str) -> bool:
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    def get_session_history(self, session_id: str) -> List[Message]:
        session = self._sessions.get(session_id)
        return session.messages if session else []


# Singleton instance for the service
_conversation_service: Optional[ConversationService] = None


def get_conversation_service() -> ConversationService:
    global _conversation_service
    if _conversation_service is None:
        _conversation_service = ConversationService()
    return _conversation_service
