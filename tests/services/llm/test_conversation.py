"""Tests for the ConversationService."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.llm.conversation import (
    ChatResponse,
    ConversationContext,
    ConversationService,
    Message,
    get_conversation_service,
)


class TestMessage:
    """Tests for Message model."""

    def test_message_creation(self):
        """Test creating a message."""
        msg = Message(role="user", content="Hello")
        assert msg.role == "user"
        assert msg.content == "Hello"
        assert msg.timestamp is not None

    def test_message_with_timestamp(self):
        """Test message with explicit timestamp."""
        ts = datetime(2024, 1, 1, 12, 0, 0)
        msg = Message(role="assistant", content="Hi there!", timestamp=ts)
        assert msg.timestamp == ts


class TestConversationContext:
    """Tests for ConversationContext model."""

    def test_context_creation(self):
        """Test creating a context."""
        ctx = ConversationContext(session_id="test-123")
        assert ctx.session_id == "test-123"
        assert ctx.messages == []
        assert ctx.data_summary is None
        assert ctx.metrics_summary is None

    def test_context_with_data(self):
        """Test context with data summary."""
        ctx = ConversationContext(
            session_id="test-456", data_summary="Test data summary", metrics_summary="Test metrics"
        )
        assert ctx.data_summary == "Test data summary"
        assert ctx.metrics_summary == "Test metrics"


class TestConversationService:
    """Tests for ConversationService."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing."""
        with patch("app.services.llm.conversation.get_settings") as mock:
            settings = MagicMock()
            settings.DEEPSEEK_API_KEY = ""
            settings.OPENAI_API_KEY = "test-key"
            settings.DEEPSEEK_BASE_URL = "https://api.deepseek.com"
            settings.DEEPSEEK_MODEL = "deepseek-chat"
            mock.return_value = settings
            yield mock

    @pytest.fixture
    def service(self, mock_settings):
        """Create a ConversationService for testing."""
        with patch("app.services.llm.conversation.AsyncOpenAI"):
            return ConversationService()

    def test_get_or_create_session_new(self, service):
        """Test creating a new session."""
        session = service.get_or_create_session("new-session")
        assert session.session_id == "new-session"
        assert len(session.messages) == 0

    def test_get_or_create_session_existing(self, service):
        """Test getting existing session."""
        # Create session
        service.get_or_create_session("existing-session")
        # Add a message
        service._sessions["existing-session"].messages.append(Message(role="user", content="test"))

        # Get same session
        session = service.get_or_create_session("existing-session")
        assert len(session.messages) == 1

    def test_update_data_context(self, service):
        """Test updating data context."""
        service.update_data_context(
            session_id="ctx-session",
            data_summary="Data summary here",
            metrics_summary="Metrics summary here",
        )

        session = service._sessions["ctx-session"]
        assert session.data_summary == "Data summary here"
        assert session.metrics_summary == "Metrics summary here"

    def test_update_data_context_partial(self, service):
        """Test partial context update."""
        service.update_data_context(session_id="partial-session", data_summary="Initial data")
        service.update_data_context(session_id="partial-session", metrics_summary="Added metrics")

        session = service._sessions["partial-session"]
        assert session.data_summary == "Initial data"
        assert session.metrics_summary == "Added metrics"

    def test_format_conversation_history_empty(self, service):
        """Test formatting empty history."""
        result = service._format_conversation_history([])
        assert result == "This is the start of the conversation."

    def test_format_conversation_history(self, service):
        """Test formatting conversation history."""
        messages = [
            Message(role="user", content="Hello"),
            Message(role="assistant", content="Hi there!"),
            Message(role="user", content="How are you?"),
        ]

        result = service._format_conversation_history(messages)

        assert "User: Hello" in result
        assert "Echo: Hi there!" in result
        assert "User: How are you?" in result

    def test_format_conversation_history_truncation(self, service):
        """Test that history is truncated to max messages."""
        messages = [Message(role="user", content=f"Message {i}") for i in range(20)]

        result = service._format_conversation_history(messages, max_messages=5)
        lines = result.split("\n")
        assert len(lines) == 5

    def test_clear_session_exists(self, service):
        """Test clearing existing session."""
        service.get_or_create_session("to-clear")
        result = service.clear_session("to-clear")
        assert result is True
        assert "to-clear" not in service._sessions

    def test_clear_session_not_exists(self, service):
        """Test clearing non-existent session."""
        result = service.clear_session("does-not-exist")
        assert result is False

    def test_get_session_history(self, service):
        """Test getting session history."""
        session = service.get_or_create_session("history-session")
        session.messages.append(Message(role="user", content="Test message"))

        history = service.get_session_history("history-session")
        assert len(history) == 1
        assert history[0].content == "Test message"

    def test_get_session_history_not_exists(self, service):
        """Test getting history for non-existent session."""
        history = service.get_session_history("no-such-session")
        assert history == []

    @pytest.mark.asyncio
    async def test_chat(self, service):
        """Test chat method with mocked LLM."""
        # Mock the OpenAI client response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Hello! I'm Echo, your data consultant."
        mock_response.usage = MagicMock()
        mock_response.usage.total_tokens = 100

        service.client.chat.completions.create = AsyncMock(return_value=mock_response)

        response = await service.chat(session_id="chat-test", user_message="Hi!")

        assert isinstance(response, ChatResponse)
        assert response.message == "Hello! I'm Echo, your data consultant."
        assert response.session_id == "chat-test"
        assert response.tokens_used == 100

        # Check that messages were stored
        session = service._sessions["chat-test"]
        assert len(session.messages) == 2
        assert session.messages[0].role == "user"
        assert session.messages[1].role == "assistant"

    @pytest.mark.asyncio
    async def test_chat_with_context(self, service):
        """Test chat with data context."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Your revenue is $50,000."
        mock_response.usage = None

        service.client.chat.completions.create = AsyncMock(return_value=mock_response)

        response = await service.chat(
            session_id="context-chat",
            user_message="What's my revenue?",
            data_summary="Revenue data loaded",
            metrics_summary="Total Revenue: $50,000",
        )

        assert response.message == "Your revenue is $50,000."

        # Verify context was stored
        session = service._sessions["context-chat"]
        assert session.data_summary == "Revenue data loaded"
        assert session.metrics_summary == "Total Revenue: $50,000"


class TestGetConversationService:
    """Tests for the singleton getter."""

    def test_singleton(self):
        """Test that get_conversation_service returns singleton."""
        # Reset the singleton
        import app.services.llm.conversation as conv_module

        conv_module._conversation_service = None

        with patch("app.services.llm.conversation.get_settings") as mock_settings:
            settings = MagicMock()
            settings.DEEPSEEK_API_KEY = ""
            settings.OPENAI_API_KEY = "test-key"
            settings.DEEPSEEK_BASE_URL = ""
            settings.DEEPSEEK_MODEL = "gpt-4"
            mock_settings.return_value = settings

            with patch("app.services.llm.conversation.AsyncOpenAI"):
                service1 = get_conversation_service()
                service2 = get_conversation_service()

                assert service1 is service2
