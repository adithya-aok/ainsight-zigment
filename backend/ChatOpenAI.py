
import re
import os
from typing import Any, Dict, Optional, Union, Iterator, AsyncIterator
from pydantic import Field, SecretStr
import openai

def from_env(key: str, default: Any = None) -> Any:
    """Get value from environment variable."""
    return os.getenv(key, default)


def secret_from_env(key: str, default: Any = None) -> Any:
    """Get secret value from environment variable."""
    value = os.getenv(key, default)
    if value is None:
        return None
    return SecretStr(value)


class BaseMessage:


    def __init__(self, content: str, **kwargs):
        self.content = content
        self.additional_kwargs = kwargs.get('additional_kwargs', {})
        self.response_metadata = kwargs.get('response_metadata', {})
        self.type = kwargs.get('type', 'base')
        self.name = kwargs.get('name', None)
        self.id = kwargs.get('id', None)

    @property
    def text(self) -> str:
        """Get the text content of the message."""
        return str(self.content)

    def __str__(self) -> str:
        """String representation of the message."""
        return str(self.content)

    def __repr__(self) -> str:
        """Representation of the message."""
        return f"{self.__class__.__name__}(content={repr(self.content)})"


class AIMessage(BaseMessage):
    """AI message implementation."""

    def __init__(self, content: str, **kwargs):
        super().__init__(content, type='ai', **kwargs)


class AIMessageChunk(BaseMessage):
    """AI message chunk implementation."""

    def __init__(self, content: str, **kwargs):
        super().__init__(content, type='ai', **kwargs)
        self.chunk_position = kwargs.get('chunk_position', None)

    def __add__(self, other):
        """Concatenate message chunks."""
        if isinstance(other, AIMessageChunk):
            return AIMessageChunk(
                content=self.content + other.content,
                additional_kwargs={**self.additional_kwargs, **other.additional_kwargs},
                response_metadata={**self.response_metadata, **other.response_metadata}
            )
        return self



class Generation:
    """Minimal generation implementation."""
    
    def __init__(self, text="", generation_info=None):
        self.text = text
        self.generation_info = generation_info or {}
        self.type = "Generation"
    
    def __add__(self, other):
        """Concatenate two generations."""
        if isinstance(other, Generation):
            return Generation(
                text=self.text + other.text,
                generation_info={**self.generation_info, **other.generation_info}
            )
        raise TypeError(f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'")


class ChatGeneration(Generation):
    """Minimal chat generation implementation."""
    
    def __init__(self, message, generation_info=None):
        self.message = message
        self.generation_info = generation_info or {}
        self.type = "ChatGeneration"
        # Set text from message content
        if hasattr(message, 'content'):
            self.text = str(message.content)
        else:
            self.text = str(message)
    
    def __add__(self, other):
        """Concatenate two chat generations."""
        if isinstance(other, ChatGeneration):
            return ChatGeneration(
                message=self.message + other.message,
                generation_info={**self.generation_info, **other.generation_info}
            )
        raise TypeError(f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'")


class ChatGenerationChunk(ChatGeneration):
    """Minimal chat generation chunk implementation."""
    
    def __init__(self, message, generation_info=None):
        super().__init__(message, generation_info)
        self.type = "ChatGenerationChunk"
    
    def __add__(self, other):
        """Concatenate chat generation chunks."""
        if isinstance(other, ChatGenerationChunk):
            return ChatGenerationChunk(
                message=self.message + other.message,
                generation_info={**self.generation_info, **other.generation_info}
            )
        elif isinstance(other, list) and all(isinstance(x, ChatGenerationChunk) for x in other):
            # Handle list of chunks
            result = self
            for chunk in other:
                result = result + chunk
            return result
        raise TypeError(f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'")


class ChatResult:
    """Minimal chat result implementation."""
    
    def __init__(self, generations, llm_output=None):
        self.generations = generations
        self.llm_output = llm_output or {}


class Runnable:
    """Minimal runnable implementation."""
    
    def __init__(self, func=None):
        self.func = func
    
    def invoke(self, input_data, **kwargs):
        """Invoke the runnable."""
        if self.func:
            return self.func(input_data, **kwargs)
        return input_data
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))


class ChatPromptTemplate:
    """Minimal chat prompt template implementation."""
    
    def __init__(self, messages=None):
        self.messages = messages or []
    
    @classmethod
    def from_template(cls, template):
        """Create from template string."""
        return cls([{"role": "user", "content": template}])
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.format(**x)))
    
    def format(self, **kwargs):
        """Format the template with variables."""
        formatted = []
        for message in self.messages:
            if isinstance(message, dict):
                content = message.get("content", "")
                for key, value in kwargs.items():
                    content = content.replace(f"{{{key}}}", str(value))
                formatted.append({**message, "content": content})
            else:
                formatted.append(message)
        return formatted
    
    def invoke(self, input_data):
        """Invoke the template with input data."""
        return self.format(**input_data)
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))


class StrOutputParser:
    """Minimal string output parser implementation."""
    
    def parse(self, text):
        """Parse input to string - returns the input text with no changes."""
        return str(text)
    
    def invoke(self, input_data):
        """Parse input to string."""
        try:
            # Handle message objects
            if hasattr(input_data, 'content'):
                return str(input_data.content)
            
            # Handle dictionaries
            elif isinstance(input_data, dict):
                if 'content' in input_data:
                    return str(input_data['content'])
                elif 'text' in input_data:
                    return str(input_data['text'])
                else:
                    # Try to find any string value in the dict
                    for key, value in input_data.items():
                        if isinstance(value, str) and value.strip():
                            return str(value)
                    return str(input_data)
            
            # Handle lists
            elif isinstance(input_data, list) and len(input_data) > 0:
                # Handle list of messages - extract content from first message
                first_msg = input_data[0]
                if isinstance(first_msg, dict) and 'content' in first_msg:
                    return str(first_msg['content'])
                elif hasattr(first_msg, 'content'):
                    return str(first_msg.content)
                else:
                    return str(first_msg)
            
            # Handle other types
            else:
                return str(input_data)
                
        except Exception as e:
            # Fallback: try to convert to string
            try:
                return str(input_data)
            except Exception:
                return ""
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))


class RunnableAssign:
    """Minimal runnable assign implementation."""
    
    def __init__(self, assignments):
        self.assignments = assignments
    
    def invoke(self, input_data):
        """Assign new fields to the input data."""
        if not isinstance(input_data, dict):
            raise ValueError("The input to RunnablePassthrough.assign() must be a dict.")
        
        # Start with the input data
        result = input_data.copy()
        
        # Add or update fields
        for key, value in self.assignments.items():
            if callable(value):
                # If value is a function, call it with input_data
                result[key] = value(input_data)
            else:
                # If value is not a function, use it directly
                result[key] = value
        
        return result
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))


class RunnablePassthrough:
    """Minimal runnable passthrough implementation."""
    
    def invoke(self, input_data):
        """Pass through input unchanged."""
        return input_data
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))
    
    @classmethod
    def assign(cls, **kwargs):
        """Assign new fields to the input data."""
        return RunnableAssign(kwargs)


# Type aliases for minimal implementation
LanguageModelInput = Union[str, list, Any]
LanguageModelOutput = Union[BaseMessage, str]


class BaseLanguageModel:
    """Minimal base language model implementation for essential functionality."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def generate_prompt(self, prompts, stop=None, callbacks=None, **kwargs):
        """Generate from prompts - to be implemented by subclasses."""
        raise NotImplementedError

    async def agenerate_prompt(self, prompts, stop=None, callbacks=None, **kwargs):
        """Async generate from prompts - to be implemented by subclasses."""
        raise NotImplementedError

    def with_structured_output(self, schema, **kwargs):
        """Structured output - to be implemented by subclasses."""
        raise NotImplementedError

    @property
    def _identifying_params(self):
        """Get identifying parameters."""
        return getattr(self, 'lc_attributes', {})

    def get_token_ids(self, text: str) -> list[int]:
        """Get token IDs for text."""
        # Simple implementation - just return character count as proxy
        return list(range(len(text)))

    def get_num_tokens(self, text: str) -> int:
        """Get number of tokens in text."""
        return len(self.get_token_ids(text))

    def get_num_tokens_from_messages(self, messages, tools=None) -> int:
        """Get number of tokens from messages."""
        total = 0
        for message in messages:
            if hasattr(message, 'content'):
                total += self.get_num_tokens(str(message.content))
            else:
                total += self.get_num_tokens(str(message))
        return total


class BaseChatModel:
    """Minimal base chat model implementation for essential functionality."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _convert_input(self, model_input: LanguageModelInput) -> Any:
        """Convert input to messages."""
        if isinstance(model_input, str):
            return [BaseMessage(content=model_input, type='user')]
        if isinstance(model_input, list):
            # Convert list of dicts to list of message objects
            messages = []
            for item in model_input:
                if isinstance(item, dict):
                    messages.append(BaseMessage(
                        content=item.get('content', ''),
                        type=item.get('role', 'user')
                    ))
                elif isinstance(item, BaseMessage):
                    messages.append(item)
                else:
                    messages.append(BaseMessage(content=str(item), type='user'))
            return messages
        return model_input

    def invoke(self, input: LanguageModelInput, **kwargs) -> AIMessage:
        """Invoke the model with input."""
        messages = self._convert_input(input)
        result = self._generate(messages, **kwargs)
        return result.generations[0].message

    def bind(self, **kwargs):
        """Bind additional parameters to the model."""
        return self
    
    def __or__(self, other):
        """Pipe operator for chaining."""
        return Runnable(lambda x: other.invoke(self.invoke(x)))

    def _generate(self, messages, **kwargs) -> ChatResult:
        """Generate response - to be implemented by subclasses."""
        raise NotImplementedError

    @property
    def _llm_type(self) -> str:
        """Return type of chat model."""
        return "base-chat"


class BaseChatOpenAI(BaseChatModel):
    """Simplified base wrapper around OpenAI large language models for chat."""

    model_name: str = Field(default="gpt-3.5-turbo", alias="model")
    temperature: Optional[float] = None
    max_tokens: Optional[int] = Field(default=None)
    stop: Optional[Union[list[str], str]] = Field(default=None, alias="stop_sequences")
    
    # OpenAI client attributes
    client: Any = Field(default=None, exclude=True)
    async_client: Any = Field(default=None, exclude=True)
    root_client: Any = Field(default=None, exclude=True)
    root_async_client: Any = Field(default=None, exclude=True)
    
    # API configuration
    openai_api_key: Optional[SecretStr] = Field(
        alias="api_key", default_factory=secret_from_env("OPENAI_API_KEY", default=None)
    )
    openai_api_base: Optional[str] = Field(default=None, alias="base_url")
    openai_organization: Optional[str] = Field(default=None, alias="organization")
    request_timeout: Union[float, tuple[float, float], Any, None] = Field(
        default=None, alias="timeout"
    )

    def __init__(self, **kwargs):
        # Call parent init first
        super().__init__(**kwargs)
        
        # Override any FieldInfo objects with actual values
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_organization = None
        self.openai_api_base = None
        self.request_timeout = None
        
        self._setup_clients()

    def _setup_clients(self):
        """Setup OpenAI clients."""
        # Get API key from environment
        api_key = os.getenv("OPENAI_API_KEY")
        
        # Initialize OpenAI client with minimal parameters
        client_params = {
            "api_key": api_key,
        }

        # Initialize OpenAI client
        self.root_client = openai.OpenAI(**client_params)
        self.client = self.root_client.chat.completions
        
        # Initialize async client
        self.root_async_client = openai.AsyncOpenAI(**client_params)
        self.async_client = self.root_async_client.chat.completions

    @property
    def _default_params(self) -> dict[str, Any]:
        """Get the default parameters for calling OpenAI API."""
        params = {
            "model": self.model_name,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "stop": self.stop or None,
        }
        return {k: v for k, v in params.items() if v is not None}

    def _get_request_payload(self, input_: LanguageModelInput, *, stop: Optional[list[str]] = None, **kwargs: Any) -> dict:
        """Get request payload for OpenAI API."""
        messages = self._convert_input(input_)
        if stop is not None:
            kwargs["stop"] = stop

        payload = {**self._default_params, **kwargs}
        payload["messages"] = [
            self._convert_message_to_dict(m) for m in messages
        ]
        return payload

    def _convert_message_to_dict(self, message: BaseMessage) -> dict:
        """Convert a message to a dictionary."""
        if isinstance(message, AIMessage):
            return {"role": "assistant", "content": message.content}
        elif hasattr(message, 'role') and hasattr(message, 'content'):
            return {"role": message.role, "content": message.content}
        else:
            # Default to user role
            return {"role": "user", "content": str(message.content)}

    def _generate(self, messages: list[BaseMessage], stop: Optional[list[str]] = None, run_manager: Optional[Any] = None, **kwargs: Any) -> ChatResult:
        """Generate response from OpenAI."""
        payload = self._get_request_payload(messages, stop=stop, **kwargs)
        try:
            response = self.client.create(**payload)
        except Exception as e:
            raise e 
        return self._create_chat_result(response)

    def _create_chat_result(self, response: Any) -> ChatResult:
        """Create ChatResult from OpenAI response."""
        generations = []
        response_dict = response if isinstance(response, dict) else response.model_dump()
        
        if response_dict.get("error"):
            raise ValueError(response_dict.get("error"))

        choices = response_dict.get("choices", [])
        if choices is None:
            raise TypeError("Received response with null value for `choices`.")

        for res in choices:
            message = AIMessage(content=res["message"]["content"])
            generation_info = {}
            generation_info["finish_reason"] = res.get("finish_reason")
            gen = ChatGeneration(message=message, generation_info=generation_info)
            generations.append(gen)
        
        llm_output = {
            "model_provider": "openai",
            "model_name": response_dict.get("model", self.model_name),
        }
        if "id" in response_dict:
            llm_output["id"] = response_dict["id"]

        return ChatResult(generations=generations, llm_output=llm_output)

    @property
    def _llm_type(self) -> str:
        """Return type of chat model."""
        return "openai-chat"


class ChatOpenAI(BaseChatOpenAI):
    """Simplified OpenAI chat model integration focused on essential functionality."""

    max_tokens: Optional[int] = Field(default=None, alias="max_completion_tokens")
    """Maximum number of tokens to generate."""

    @property
    def lc_secrets(self) -> dict[str, str]:
        """Mapping of secret environment variables."""
        return {"openai_api_key": "OPENAI_API_KEY"}

    @classmethod
    def get_lc_namespace(cls) -> list[str]:
        """Get the namespace of the langchain object."""
        return ["langchain", "chat_models", "openai"]

    @property
    def lc_attributes(self) -> dict[str, Any]:
        """Get the attributes of the langchain object."""
        attributes: dict[str, Any] = {}

        if hasattr(self, 'openai_organization') and self.openai_organization:
            attributes["openai_organization"] = self.openai_organization

        if hasattr(self, 'openai_api_base') and self.openai_api_base:
            attributes["openai_api_base"] = self.openai_api_base

        return attributes

    @classmethod
    def is_lc_serializable(cls) -> bool:
        """Return whether this model can be serialized by LangChain."""
        return True

    @property
    def _default_params(self) -> dict[str, Any]:
        """Get the default parameters for calling OpenAI API."""
        params = super()._default_params
        if "max_tokens" in params:
            params["max_completion_tokens"] = params.pop("max_tokens")
        return params

    def _get_request_payload(self, input_: LanguageModelInput, *, stop: Optional[list[str]] = None, **kwargs: Any) -> dict:
        """Get request payload for OpenAI API."""
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        # max_tokens was deprecated in favor of max_completion_tokens
        if "max_tokens" in payload:
            payload["max_completion_tokens"] = payload.pop("max_tokens")

        # Mutate system message role to "developer" for o-series models
        if self.model_name and isinstance(self.model_name, str) and re.match(r"^o\d", self.model_name):
            for message in payload.get("messages", []):
                if message["role"] == "system":
                    message["role"] = "developer"
        
        # Clean up any FieldInfo objects in the payload
        cleaned_payload = {}
        for key, value in payload.items():
            if hasattr(value, '__class__') and 'FieldInfo' in str(value.__class__):
                # Skip FieldInfo objects
                continue
            cleaned_payload[key] = value
        
        # Ensure required arguments are present
        if 'model' not in cleaned_payload:
            # Convert model_name to string if it's a FieldInfo object
            model_name = self.model_name
            if hasattr(model_name, '__class__') and 'FieldInfo' in str(model_name.__class__):
                model_name = 'gpt-4o-mini'  # Default model
            cleaned_payload['model'] = model_name
        if 'messages' not in cleaned_payload:
            cleaned_payload['messages'] = []
        
        # Debug: print the payload (commented out to reduce terminal verbosity)
        # print(f"DEBUG: payload keys: {list(cleaned_payload.keys())}")
        # print(f"DEBUG: payload model: {cleaned_payload.get('model')}")
        # Print only message count instead of full messages
        print(f"🔧 LLM call: model={cleaned_payload.get('model')}, messages={len(cleaned_payload.get('messages', []))}")
        
        return cleaned_payload

    def with_structured_output(self, schema: Optional[Union[dict, type]] = None, *, method: str = "json_schema", include_raw: bool = False, strict: Optional[bool] = None, **kwargs: Any) -> Runnable:
        """Model wrapper that returns outputs formatted to match the given schema."""
        # Simple implementation - just return the model itself
        return self