from google.adk.agents import Agent, BaseAgent
from google.adk.models import llm_request, llm_response
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from litellm import LiteLLM

from typing import Optional

# temporal constants

LOCAL_LLM_API_KEY = (
    "sk-710c5429c02e1be4660cbc98fd3398b8350e060d7db6cc06187a62a5dad6e29d"
)
LOCAL_MODEL = "Qwen3-4B-Instruct-2507-Q8_0.gguf"
LOCAL_THINKING_MODEL = "DeepSeek-R1-0528-Qwen3-8B-UD-Q4_K_XL.gguf"
LOCAL_ENDPOINT = "http://127.0.0.1:8091/v1"

req = llm_request.LlmRequest
resp = llm_response.LlmResponse

print(f"llm request: {req.__doc__}")
print(f"llm response: {resp.__doc__}")

# lazy initialization
agent: Optional[BaseAgent] = None
_session: Optional[InMemorySessionService] = None
_runner: Optional[Runner] = None

agent_config = types.GenerateContentConfig


def build_runtime() -> Runner:
    # access global variables
    global agent, _session, _runner

    # setup model
    model = LiteLLM(
        model=LOCAL_THINKING_MODEL, base_url=LOCAL_ENDPOINT, api_key="sk-no-key"
    )

    agent = Agent(
        name="General agent",
        model=model,
        static_instruction="You are helpful assistant.",
        generate_content_config=None,
        mode="chat",
    )
    _session = InMemorySessionService()
    _runner = Runner(agent=agent, session_service=_session)

    return _runner
