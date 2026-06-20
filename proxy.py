import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import aiohttp
import uvicorn

app = FastAPI()

# Address where LM Studio is running
LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
# Interval in seconds for sending keep-alive ping
PING_INTERVAL = 20.0

@app.post("/v1/chat/completions")
async def proxy_chat_completions(request: Request):
    # Load the request body from GitHub Copilot
    body = await request.json()
    
    print(f"[PROXY] Received request from Copilot")
    print(f"[PROXY] Forwarding to LM Studio at {LM_STUDIO_URL}")
    print(f"[PROXY] Body keys: {list(body.keys())}")

    async def event_generator():
        # Create a queue for sharing data between fetching from LM Studio and sending to the client
        queue = asyncio.Queue()

        async def fetch_from_lm_studio():
            """Background task that fetches data from LM Studio and puts it into the queue."""
            try:
                print(f"[PROXY] Connecting to LM Studio...")
                
                connector = aiohttp.TCPConnector(force_close=False)
                timeout = aiohttp.ClientTimeout(total=300)
                
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    headers = {
                        "Content-Type": "application/json",
                        "Accept": "text/event-stream"
                    }
                    
                    async with session.post(LM_STUDIO_URL, json=body, headers=headers) as response:
                        print(f"[PROXY] LM Studio responded with status {response.status}")
                        
                        if response.status != 200:
                            error_text = await response.text()
                            error_msg = f"LM Studio returned status {response.status}: {error_text[:200]}"
                            print(f"[PROXY] ERROR: {error_msg}")
                            await queue.put(("error", error_msg.encode('utf-8')))
                            return
                        
                        # Stream data from LM Studio
                        chunk_count = 0
                        async for chunk in response.content.iter_any():
                            if chunk:
                                chunk_count += 1
                                await queue.put(("data", chunk))
                        
                        print(f"[PROXY] Received {chunk_count} chunks from LM Studio")
                    
                # Signal successful end of stream
                print(f"[PROXY] LM Studio request completed successfully")
                await queue.put(("done", None))
                
            except aiohttp.ClientConnectorError as e:
                error_msg = f"Cannot connect to LM Studio at {LM_STUDIO_URL}. Is it running? Error: {str(e)}"
                print(f"[PROXY] CONNECTION ERROR: {error_msg}")
                await queue.put(("error", str(error_msg).encode('utf-8')))
            except asyncio.TimeoutError as e:
                error_msg = f"Timeout connecting to LM Studio: {str(e)}"
                print(f"[PROXY] TIMEOUT ERROR: {error_msg}")
                await queue.put(("error", str(error_msg).encode('utf-8')))
            except Exception as e:
                error_msg = f"Proxy error while fetching from LM Studio: {type(e).__name__}: {str(e)}"
                print(f"[PROXY] ERROR: {error_msg}")
                await queue.put(("error", str(error_msg).encode('utf-8')))

        # Start fetching in the background
        fetch_task = asyncio.create_task(fetch_from_lm_studio())

        try:
            while True:
                try:
                    status, chunk = await asyncio.wait_for(queue.get(), timeout=PING_INTERVAL)

                    if status == "done":
                        break
                    elif status == "error":
                        yield b'data: {"error": "' + chunk + b'"}\n\n'
                        break

                    yield chunk
                    queue.task_done()

                except asyncio.TimeoutError:
                    print(f"[PROXY] Keep-alive ping (no data from LM Studio for {PING_INTERVAL}s)")
                    yield b"\n"
        finally:
            if not fetch_task.done():
                print(f"[PROXY] Client disconnected, canceling fetch task")
                fetch_task.cancel()

    return StreamingResponse(
        event_generator(), 
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

if __name__ == "__main__":
    print("[PROXY] Starting proxy server on http://127.0.0.1:8080")
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")