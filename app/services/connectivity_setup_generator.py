"""
Connectivity Setup Generator — Platform-specific config for external AI tools.

Generates step-by-step setup instructions with exact config blocks, validation
checkpoints, and troubleshooting tips for each supported platform.

Phase: BQ-MCP-RAG Phase 2 — allAI Connectivity Concierge
Created: S136
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://localhost:8100"

SUPPORTED_PLATFORMS = {
    "claude_desktop",
    "chatgpt_desktop",
    "cursor",
    "gemini",
    "vscode",
    "openai_custom_gpt",
    "generic_rest",
    "generic_llm",
}


class ConnectivitySetupGenerator:
    """Generate platform-specific setup instructions for external AI connectivity."""

    def generate(
        self,
        platform: str,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        datasets: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Generate setup instructions for the given platform.

        Args:
            platform: Target platform identifier.
            token: Full connectivity token (vzmcp_...).
            base_url: Base URL of the vectorAIz instance.
            datasets: Optional list of dataset info dicts for generic_llm prompts.

        Returns:
            Dict with keys: platform, title, steps, config, troubleshooting, notes.
        """
        generators = {
            "claude_desktop": self._claude_desktop,
            "chatgpt_desktop": self._chatgpt_desktop,
            "cursor": self._cursor,
            "gemini": self._gemini,
            "vscode": self._vscode,
            "openai_custom_gpt": self._openai_custom_gpt,
            "generic_rest": self._generic_rest,
            "generic_llm": self._generic_llm,
        }

        gen = generators.get(platform)
        if not gen:
            return {
                "platform": platform,
                "title": "Unknown Platform",
                "steps": [{"step": 1, "instruction": f"Platform '{platform}' is not supported."}],
                "config": None,
                "troubleshooting": [],
                "notes": [],
            }

        return gen(token=token, base_url=base_url, datasets=datasets or [])

    def _claude_desktop(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        config = {
            "mcpServers": {
                "vectoraiz": {
                    "command": "docker",
                    "args": [
                        "exec", "-i", "vectoraiz", "python",
                        "-m", "app.mcp_server", "--token", token,
                    ],
                }
            }
        }

        return {
            "platform": "claude_desktop",
            "title": "Claude Desktop MCP Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Open Claude Desktop settings",
                    "detail": "Click the Claude menu → Settings → Developer → Edit Config.",
                },
                {
                    "step": 2,
                    "instruction": "Add the vectorAIz MCP server config",
                    "detail": (
                        "Copy the config block below and paste it into your "
                        "claude_desktop_config.json file."
                    ),
                    "validation": "Verify the file saves without JSON syntax errors.",
                },
                {
                    "step": 3,
                    "instruction": "Restart Claude Desktop",
                    "detail": "Quit and reopen Claude Desktop for the new MCP server to load.",
                    "validation": "After restart, you should see 'vectoraiz' in the MCP server list.",
                },
                {
                    "step": 4,
                    "instruction": "Test the connection",
                    "detail": (
                        "In Claude Desktop, try asking: 'What datasets are available in vectorAIz?' "
                        "Claude should use the vectoraiz MCP tools to answer."
                    ),
                    "validation": "You should see dataset names from your vectorAIz instance.",
                },
            ],
            "config": config,
            "config_path": {
                "macos": "~/Library/Application Support/Claude/claude_desktop_config.json",
                "windows": "%APPDATA%/Claude/claude_desktop_config.json",
            },
            "troubleshooting": [
                "Ensure Docker is running and the vectoraiz container is active.",
                "Check that the token is valid by running connectivity_test.",
                "If Claude Desktop doesn't show the MCP server, check the JSON syntax in your config file.",
                "Verify the container name matches — run 'docker ps' to confirm.",
            ],
            "notes": [
                "This token is a secret — do not share it publicly.",
                "Each AI tool should have its own token for easy revocation.",
            ],
        }

    def _chatgpt_desktop(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        config = {
            "mcpServers": {
                "vectoraiz": {
                    "command": "docker",
                    "args": [
                        "exec", "-i", "vectoraiz", "python",
                        "-m", "app.mcp_server", "--token", token,
                    ],
                }
            }
        }

        return {
            "platform": "chatgpt_desktop",
            "title": "ChatGPT Desktop MCP Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Open ChatGPT Desktop settings",
                    "detail": "Go to Settings → Beta Features → enable MCP Servers.",
                },
                {
                    "step": 2,
                    "instruction": "Add the vectorAIz MCP server",
                    "detail": (
                        "In the MCP Servers section, add a new server with the config below. "
                        "ChatGPT Desktop uses the same MCP format as Claude Desktop."
                    ),
                    "validation": "The server should appear in your MCP servers list.",
                },
                {
                    "step": 3,
                    "instruction": "Restart ChatGPT Desktop",
                    "detail": "Quit and reopen ChatGPT Desktop to load the new MCP server.",
                    "validation": "The vectoraiz MCP server should show as connected.",
                },
                {
                    "step": 4,
                    "instruction": "Test the connection",
                    "detail": "Ask ChatGPT: 'List my vectorAIz datasets.' It should use the MCP tools.",
                    "validation": "You should see your dataset names in the response.",
                },
            ],
            "config": config,
            "config_path": {
                "macos": "~/Library/Application Support/com.openai.chat/mcp_config.json",
                "windows": "%APPDATA%/com.openai.chat/mcp_config.json",
            },
            "troubleshooting": [
                "Ensure MCP support is enabled in ChatGPT Desktop beta features.",
                "Check that Docker is running and the vectoraiz container is active.",
                "Verify the token is valid with connectivity_test.",
            ],
            "notes": [
                "This token is a secret — do not share it publicly.",
                "Each AI tool should have its own token for easy revocation.",
            ],
        }

    def _cursor(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        config = {
            "mcpServers": {
                "vectoraiz": {
                    "command": "docker",
                    "args": [
                        "exec", "-i", "vectoraiz", "python",
                        "-m", "app.mcp_server", "--token", token,
                    ],
                }
            }
        }

        return {
            "platform": "cursor",
            "title": "Cursor MCP Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Open Cursor settings",
                    "detail": "Go to Cursor Settings → Features → MCP Servers.",
                },
                {
                    "step": 2,
                    "instruction": "Add MCP server",
                    "detail": (
                        "Click 'Add MCP Server' and paste the config below. "
                        "Alternatively, add to .cursor/mcp.json in your project root."
                    ),
                    "validation": "The server should appear in the MCP servers list.",
                },
                {
                    "step": 3,
                    "instruction": "Enable the server",
                    "detail": "Toggle the vectoraiz server to 'enabled' in the MCP settings.",
                    "validation": "The server status should show as connected.",
                },
                {
                    "step": 4,
                    "instruction": "Test the connection",
                    "detail": (
                        "In Cursor chat, ask: 'Use the vectoraiz MCP to list my datasets.' "
                        "Cursor should invoke the MCP tools."
                    ),
                    "validation": "You should see dataset info from your vectorAIz instance.",
                },
            ],
            "config": config,
            "config_path": {
                "project": ".cursor/mcp.json",
                "global": "~/.cursor/mcp.json",
            },
            "troubleshooting": [
                "Ensure Docker is running and the vectoraiz container is active.",
                "Check Cursor's MCP server logs for connection errors.",
                "Verify the token is valid with connectivity_test.",
            ],
            "notes": [
                "This token is a secret — do not commit it to version control.",
                "Each AI tool should have its own token for easy revocation.",
            ],
        }

    def _gemini(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        api_url = f"{base_url}/api/v1/ext"

        return {
            "platform": "gemini",
            "title": "Google Gemini Setup (REST API)",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Note the vectorAIz REST API endpoint",
                    "detail": f"Base URL: {api_url}",
                },
                {
                    "step": 2,
                    "instruction": "Configure Gemini to use vectorAIz as a data source",
                    "detail": (
                        "In Gemini's extensions or function calling setup, add the vectorAIz "
                        "REST API endpoints. Use Bearer token authentication with the token below."
                    ),
                    "validation": "Gemini should recognize the API endpoints.",
                },
                {
                    "step": 3,
                    "instruction": "Test with a query",
                    "detail": "Ask Gemini to query your vectorAIz data to confirm the integration.",
                    "validation": "You should get data from your vectorAIz datasets.",
                },
            ],
            "config": {
                "api_base_url": api_url,
                "auth_header": f"Bearer {token}",
                "endpoints": {
                    "list_datasets": f"{api_url}/datasets",
                    "search": f"{api_url}/search",
                    "sql": f"{api_url}/sql",
                    "schema": f"{api_url}/schema/{{dataset_id}}",
                },
            },
            "troubleshooting": [
                "Gemini MCP support is evolving — check Google's latest docs.",
                "For REST-based integration, ensure the vectorAIz instance is accessible from Gemini.",
                "Use the generic_rest setup as a fallback if direct integration isn't available.",
            ],
            "notes": [
                "This token is a secret — do not share it publicly.",
                "Gemini integration may require the REST API approach if MCP is not yet supported.",
            ],
        }

    def _vscode(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        config = {
            "mcpServers": {
                "vectoraiz": {
                    "command": "docker",
                    "args": [
                        "exec", "-i", "vectoraiz", "python",
                        "-m", "app.mcp_server", "--token", token,
                    ],
                }
            }
        }

        return {
            "platform": "vscode",
            "title": "VS Code (GitHub Copilot) MCP Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Open VS Code settings",
                    "detail": (
                        "Open the Command Palette (Cmd+Shift+P / Ctrl+Shift+P) and search for "
                        "'MCP: Add Server' or edit .vscode/mcp.json in your workspace."
                    ),
                },
                {
                    "step": 2,
                    "instruction": "Add the vectorAIz MCP server config",
                    "detail": "Paste the config below into your MCP settings.",
                    "validation": "The MCP server should appear in the GitHub Copilot agent list.",
                },
                {
                    "step": 3,
                    "instruction": "Test the connection",
                    "detail": (
                        "In the Copilot chat panel, use @vectoraiz to query your data: "
                        "'@vectoraiz list my datasets'."
                    ),
                    "validation": "You should see your vectorAIz datasets in the response.",
                },
            ],
            "config": config,
            "config_path": {
                "workspace": ".vscode/mcp.json",
                "user": "VS Code Settings → Extensions → GitHub Copilot → MCP Servers",
            },
            "troubleshooting": [
                "Ensure the GitHub Copilot extension is installed and up to date.",
                "Check that MCP support is enabled in Copilot settings.",
                "Verify Docker is running and the vectoraiz container is active.",
            ],
            "notes": [
                "This token is a secret — do not commit it to version control.",
                "Each AI tool should have its own token for easy revocation.",
            ],
        }

    def _openai_custom_gpt(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        api_url = f"{base_url}/api/v1/ext"

        return {
            "platform": "openai_custom_gpt",
            "title": "OpenAI Custom GPT Setup (REST API)",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Open the GPT Editor",
                    "detail": "Go to chat.openai.com → Explore GPTs → Create a GPT.",
                },
                {
                    "step": 2,
                    "instruction": "Add an Action",
                    "detail": (
                        "In the Configure tab, click 'Create new action'. "
                        "Import the OpenAPI schema from the URL below, or paste the schema manually."
                    ),
                    "validation": "The action should show 4 endpoints after import.",
                },
                {
                    "step": 3,
                    "instruction": "Configure authentication",
                    "detail": (
                        "Set Authentication type to 'API Key', Header name: 'Authorization', "
                        f"Value: 'Bearer {token}'."
                    ),
                    "validation": "The authentication section should show 'API Key' configured.",
                },
                {
                    "step": 4,
                    "instruction": "Test the GPT",
                    "detail": "Save the GPT and try asking it to list your datasets.",
                    "validation": "The GPT should call the vectorAIz API and return dataset info.",
                },
            ],
            "config": {
                "openapi_schema_url": f"{api_url}/openapi.json",
                "api_base_url": api_url,
                "auth_type": "API Key",
                "auth_header": "Authorization",
                "auth_value": f"Bearer {token}",
            },
            "troubleshooting": [
                "Ensure the vectorAIz instance is accessible from the internet (not just localhost).",
                "If using localhost, set up a tunnel (e.g., ngrok) to expose the API.",
                "Verify the token is valid with connectivity_test.",
                "Check that the OpenAPI schema loads correctly in the GPT editor.",
            ],
            "notes": [
                "This token is a secret — do not share the GPT publicly with the token embedded.",
                "Custom GPTs require the vectorAIz instance to be internet-accessible.",
                (
                    "If you're running vectorAIz locally (Docker/localhost), ChatGPT cannot reach it. "
                    "Claude Desktop via MCP is the easiest local option — it connects directly to "
                    "your Docker container with no tunneling needed."
                ),
            ],
        }

    def _generic_rest(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        api_url = f"{base_url}/api/v1/ext"

        return {
            "platform": "generic_rest",
            "title": "Generic REST API Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Note the API base URL and authentication",
                    "detail": (
                        f"Base URL: {api_url}\n"
                        f"Auth: Bearer token in Authorization header."
                    ),
                },
                {
                    "step": 2,
                    "instruction": "List datasets",
                    "detail": (
                        f"curl -H 'Authorization: Bearer {token}' "
                        f"{api_url}/datasets"
                    ),
                    "validation": "You should see a JSON response with your datasets.",
                },
                {
                    "step": 3,
                    "instruction": "Search vectors",
                    "detail": (
                        f"curl -X POST -H 'Authorization: Bearer {token}' "
                        f"-H 'Content-Type: application/json' "
                        f"-d '{{\"query\": \"your search text\", \"top_k\": 5}}' "
                        f"{api_url}/search"
                    ),
                    "validation": "You should see search results with scores.",
                },
                {
                    "step": 4,
                    "instruction": "Execute SQL",
                    "detail": (
                        f"curl -X POST -H 'Authorization: Bearer {token}' "
                        f"-H 'Content-Type: application/json' "
                        f"-d '{{\"sql\": \"SELECT * FROM dataset_<ID> LIMIT 5\"}}' "
                        f"{api_url}/sql"
                    ),
                    "validation": "You should see query results with columns and rows.",
                },
            ],
            "config": {
                "api_base_url": api_url,
                "auth_header": f"Bearer {token}",
                "endpoints": {
                    "list_datasets": {"method": "GET", "path": "/datasets"},
                    "search": {"method": "POST", "path": "/search"},
                    "sql": {"method": "POST", "path": "/sql"},
                    "schema": {"method": "GET", "path": "/schema/{dataset_id}"},
                },
            },
            "troubleshooting": [
                "If you get 401, check that the token is correct and not revoked.",
                "If you get 429, you're being rate-limited — wait and retry.",
                "If you get 503, external connectivity may be disabled.",
                "Ensure the vectorAIz instance is accessible from your client.",
            ],
            "notes": [
                "This token is a secret — treat it like a password.",
                "Each integration should use its own token for easy revocation.",
            ],
        }

    def _generic_llm(
        self, token: str, base_url: str, datasets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        api_url = f"{base_url}/api/v1/ext"

        system_prompt = self._build_llm_system_prompt(
            token=token, api_url=api_url, datasets=datasets,
        )

        return {
            "platform": "generic_llm",
            "title": "Generic LLM System Prompt Setup",
            "steps": [
                {
                    "step": 1,
                    "instruction": "Copy the system prompt below",
                    "detail": (
                        "This system prompt contains everything an LLM needs to query your "
                        "vectorAIz data: API endpoints, authentication, available datasets, "
                        "and usage instructions."
                    ),
                },
                {
                    "step": 2,
                    "instruction": "Paste as the system prompt in your LLM interface",
                    "detail": (
                        "Whether you're using a custom chatbot, API integration, or local LLM, "
                        "set this as the system/context prompt."
                    ),
                    "validation": "The LLM should be able to describe your available datasets.",
                },
                {
                    "step": 3,
                    "instruction": "Test with a query",
                    "detail": (
                        "Ask the LLM: 'What datasets are available?' It should list "
                        "the datasets shown in the system prompt and offer to query them."
                    ),
                    "validation": "The LLM should reference your actual dataset names.",
                },
            ],
            "config": {
                "system_prompt": system_prompt,
            },
            "troubleshooting": [
                "If the LLM can't make API calls, ensure it has HTTP/function-calling capability.",
                "For local LLMs, you may need to use a tool-use framework (e.g., LangChain).",
                "Verify the token is valid by running one of the curl commands manually.",
            ],
            "notes": [
                "The system prompt contains your token — treat it as confidential.",
                "Regenerate this prompt if you add or remove datasets.",
            ],
        }

    def _build_llm_system_prompt(
        self,
        token: str,
        api_url: str,
        datasets: List[Dict[str, Any]],
    ) -> str:
        """Build a dynamic system prompt that includes actual dataset info."""
        datasets_section = "No datasets currently available."
        if datasets:
            lines = []
            for ds in datasets:
                name = ds.get("name", ds.get("filename", "Unknown"))
                ds_id = ds.get("id", "?")
                rows = ds.get("row_count", ds.get("rows", "?"))
                cols = ds.get("column_count", ds.get("columns", "?"))
                desc = ds.get("description", "")
                desc_str = f" — {desc}" if desc else ""
                lines.append(f"  - {name} (table: dataset_{ds_id}, {rows} rows, {cols} cols){desc_str}")
            datasets_section = "\n".join(lines)

        return f"""You have access to a vectorAIz data instance via REST API.

## API Configuration
- Base URL: {api_url}
- Authentication: Include header `Authorization: Bearer {token}`

## Available Datasets
{datasets_section}

## Endpoints

### GET {api_url}/datasets
List all available datasets with metadata.

### POST {api_url}/search
Semantic vector search across datasets.
Body: {{"query": "natural language search", "top_k": 5, "dataset_id": "optional"}}

### POST {api_url}/sql
Execute read-only SQL queries. Tables are named dataset_<id>.
Body: {{"sql": "SELECT * FROM dataset_<id> LIMIT 10", "dataset_id": "optional"}}

### GET {api_url}/schema/{{dataset_id}}
Get column definitions for a specific dataset.

## When to Use Each Endpoint
- **Search** (POST /search): Use for fuzzy, meaning-based queries like "find records about renewable energy" or "customers in California".
- **SQL** (POST /sql): Use for exact queries with filters, aggregations, joins, or counts like "how many rows have price > 100" or "average revenue by region".
- **List datasets** (GET /datasets): Use to discover what data is available.
- **Schema** (GET /schema/{{id}}): Use to understand column names and types before writing SQL.

## Important Notes
- All queries are read-only. No INSERT, UPDATE, DELETE, or DDL.
- SQL queries have a row limit and timeout enforced server-side.
- The token is a secret — do not expose it to end users."""
