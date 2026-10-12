from asyncio import StreamWriter


class SemaphoreTimeOutError(Exception):
    """Вызываем при timeout семафора"""


class OpenConnError(Exception):
    """Вызываем при ошибке открытия соединения"""


async def send_error_response(
    writer: StreamWriter, code: int = 504, message: str = "Gateway Timeout"
):

    response = (
        f"HTTP/1.1 {code} {message}\r\n"
        "Content-Type: text/plain\r\n"
        f"Content-Length: {len(message)}\r\n"
        "Connection: close\r\n\r\n"
        f"{message}"
    )
    if writer.is_closing():
        return
    writer.write(response.encode())
    await writer.drain()
