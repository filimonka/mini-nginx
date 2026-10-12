import asyncio

from errors import OpenConnError, SemaphoreTimeOutError
from logger import logger
from settings import config


class UpstreamPool:
    def __init__(self):
        self.pool: list[tuple] = [
            (upstr.host, upstr.port) for upstr in config.upstreams
        ]
        self.last_idx = 0
        self.lock = asyncio.Lock()
        self.max_conn_per_upstream = config.limits.max_conns_per_upstream
        # key - (host, port)
        self.idle_connections: dict[tuple[str, int], asyncio.Queue] = (
            self._get_connections_queue()
        )

        self.upstream_semaphores = self.get_upstream_semaphores()

    def get_upstream_semaphores(self) -> dict[tuple[str, int], asyncio.Semaphore]:
        return {
            (host, port): asyncio.Semaphore(self.max_conn_per_upstream)
            for host, port in self.pool
        }

    def _get_connections_queue(self) -> dict[tuple[str, int], asyncio.Queue]:
        return {
            (host, port): asyncio.Queue(maxsize=self.max_conn_per_upstream)
            for host, port in self.pool
        }

    async def get_upstream(self) -> tuple[str, int]:
        async with self.lock:
            self.last_idx = (self.last_idx + 1) % len(self.pool)
            return self.pool[self.last_idx]

    async def get_connection(
        self, peer_port: int | None = None
    ) -> tuple[
        asyncio.StreamReader | None, asyncio.StreamWriter | None, tuple[str, int]
    ]:
        """returns reader, writer, the upstream address used (or None if failed)"""
        host, port = await self.get_upstream()
        semaphore = self.upstream_semaphores[(host, port)]

        try:
            async with semaphore, self.lock:
                idle_conns: asyncio.Queue = self.idle_connections.get((host, port))
                while not idle_conns.empty():
                    reader, writer = await idle_conns.get()
                    # проверяем, что соединение еще живо
                    if writer.is_closing() or reader.at_eof():
                        writer.close()
                        await writer.wait_closed()
                        continue

                    logger.info(
                        "peer: %s reused idle connection to upstream %s:%s",
                        peer_port,
                        host,
                        port,
                    )
                    return reader, writer, (host, port)
                try:
                    reader, writer = await asyncio.open_connection(host=host, port=port)
                    logger.info(
                        "peer: %s connected to upstream %s:%s", peer_port, host, port
                    )
                    return reader, writer, (host, port)
                except OSError as e:
                    error_msg = f"Error connecting to upstream {host}:{port} - {e}, peer_port: {peer_port}"
                    logger.error(error_msg)
                    raise OpenConnError(error_msg)
        except TimeoutError as e:
            error_msg = f"Timeout connecting to upstream {host}:{port} - {e}, peer_port: {peer_port}"
            logger.error(error_msg)
            raise SemaphoreTimeOutError(error_msg)

    async def release_connection(
        self,
        address: tuple[str, int],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """put a still-alive keep-alive connection back into the idle pool for reuse"""
        if writer.is_closing():
            return
        async with self.lock:
            await self.idle_connections[address].put((reader, writer))


up_pool = UpstreamPool()
