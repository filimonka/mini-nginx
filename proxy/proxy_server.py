import asyncio

from asyncio import StreamReader, StreamWriter

from client_handler import ClientHandler
from logger import logger
from metrics import Metrics


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
            await handler.handle_client()
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
            except KeyboardInterrupt:
                logger.info("Server stopped by user")
            except Exception as e:
                logger.info(e)
            finally:
                server.close()
                await server.wait_closed()
                logger.info("Server closed")
