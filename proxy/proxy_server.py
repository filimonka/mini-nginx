import asyncio
from asyncio import StreamReader, StreamWriter, TimeoutError

from client_handler import ClientHandler
from errors import send_error_response
from logger import logger
from metrics import Metrics
from settings import config


class ProxyServer:
    def __init__(self, host: str, port: int, max_connections: int, metrics: Metrics):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.semaphore = asyncio.Semaphore(self.max_connections)
        self.metrics = metrics

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        handler = ClientHandler(
            semaphore=self.semaphore, reader=reader, writer=writer, metrics=self.metrics
        )
        self.metrics.increment_active_client_count()
        self.metrics.increment_new_clients_count()
        try:
            await asyncio.wait_for(
                handler.handle_client(),
                timeout=config.timeouts.total_ms,
            )
        except TimeoutError:
            self.metrics.increment_failed_total()
            self.metrics.increment_timeout_errors()
            logger.error("Error handling client: Timeout total")
            await send_error_response(
                code=504, message="Gateway Timeout", writer=writer
            )
        except Exception as e:
            logger.error("Error handling client: %s", e)
            self.metrics.increment_failed_total()
            await send_error_response(code=500, message="Server Error", writer=writer)
        finally:
            self.metrics.decrement_active_client_count()

    async def start(self):

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            try:
                logger.info("Starting Proxy server on %s:%s", self.host, self.port)
                await server.serve_forever()
                logger.info("Proxy server started on %s:%s", self.host, self.port)
            except asyncio.CancelledError:
                logger.info("Server cancelled")
            except Exception as e:
                logger.info(e)
            finally:
                server.close()
                await server.wait_closed()
                logger.info("Server closed")
