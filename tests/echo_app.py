from fastapi import FastAPI
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
