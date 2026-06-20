# keepalive-llm-proxy

A lightweight proxy that prevents timeout issues between AI coding assistants (such as GitHub Copilot) and local LLM servers (such as LM Studio).

Tested with:
- GitHub Copilot for Visual Studio 2026
- GitHub Copilot for VS Code
- LM Studio

## Installation

```bash
pip install fastapi aiohttp uvicorn
```

## Configuration

Edit `proxy.py` if needed:

```python
LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
PING_INTERVAL = 20.0
```

## Running

```bash
python proxy.py
```

or on Windows:

```bat
run.bat
```

The proxy listens on:

```text
http://127.0.0.1:8080/v1/chat/completions
```

Configure your AI tool to use this endpoint instead of connecting directly to your LLM server.

## How It Works

If no data is received from LLM for PING_INTERVAL seconds, the proxy sends a keep-alive newline (\n).