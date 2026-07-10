from google.adk.events.event import Event
from google.genai import types

from agent.sepa_agent import FORMATTER_AGENT_NAME, RESEARCH_AGENT_NAME, _event_to_dict


def _text_event(author: str, text: str) -> Event:
    return Event(
        author=author,
        content=types.Content(role="model", parts=[types.Part(text=text)]),
    )


def test_research_final_event_becomes_progress() -> None:
    event = _text_event(RESEARCH_AGENT_NAME, "Found recipe ingredients.")

    payload = _event_to_dict(event)

    assert payload == {
        "type": "progress",
        "content": "Research complete. Structuring response...",
    }


def test_formatter_final_event_is_validated_shopping_list() -> None:
    event = _text_event(
        FORMATTER_AGENT_NAME,
        """
        ```json
        {
          "project_name": "Tiramisu para 5 personas",
          "message": "Precios pendientes de MCP.",
          "total_estimate": 0.0,
          "savings": 0.0,
          "stores": [
            {
              "name": "Estimación sin precios SEPA",
              "items": [
                {
                  "name": "Mascarpone",
                  "price": 0.0,
                  "description": "500 g",
                  "quantity": 1
                }
              ]
            }
          ]
        }
        ```
        """,
    )

    payload = _event_to_dict(event)

    assert payload["type"] == "final"
    assert payload["data"]["project_name"] == "Tiramisu para 5 personas"
    assert payload["data"]["stores"][0]["items"][0]["quantity"] == 1


def test_formatter_invalid_shopping_list_becomes_error() -> None:
    event = _text_event(
        FORMATTER_AGENT_NAME,
        """
        {
          "project_name": "Tiramisu",
          "message": "No data.",
          "total_estimate": 0.0,
          "savings": 0.0,
          "stores": []
        }
        """,
    )

    payload = _event_to_dict(event)

    assert payload["type"] == "error"
    assert "invalid ShoppingList JSON" in payload["content"]
