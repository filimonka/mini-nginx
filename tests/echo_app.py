import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


app = FastAPI(
    docs_url="/docs",
    redoc_url="/redoc",
    title="Mini Nginx",
)


class Message(BaseModel):
    message: str


@app.get("/anything", response_model=Message)
async def get_anything():
    return Message(message="Random rubbish").model_dump()


@app.post("/something", response_model=Message)
async def post_something(message: Message):
    return Message(message=f"Received: {message.message}").model_dump()


@app.get("/stream")
async def stream():
    async def gen():
        for i in range(3):
            yield f"part {i}\n".encode()
            await asyncio.sleep(0.2)

    return StreamingResponse(gen(), media_type="text/plain")


@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return {"received_bytes": len(body), "body": body.decode(errors="replace")}
