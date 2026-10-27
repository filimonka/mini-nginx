import asyncio

from asyncio.streams import StreamReader, StreamWriter
from datetime import UTC, datetime

from errors import OpenConnError, send_error_response
from logger import logger
from metrics import Metrics
from settings import config
from upstream_pool import up_pool


class ClientHandler:
    def __init__(
        self,
        semaphore: asyncio.Semaphore,
        reader: StreamReader,
        writer: StreamWriter,
        metrics: Metrics,
    ):
        self.reader = reader
        self.writer = writer
        self.metrics = metrics
        self.semaphore = semaphore
        self.address = self.writer.get_extra_info("peername")
        logger.info(f"Подключился {self.address}")

    async def get_headers(
        self, reader: StreamReader, from_client: bool = True
    ) -> bytes | None:
        try:
            headers = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"), timeout=config.timeouts.read_ms
            )
            return headers

        except TimeoutError:
            logger.error(
                f"Ошибка чтения заголовков: Timeout, from_client={from_client}"
            )
            self.metrics.increment_failed_total()
            self.metrics.increment_timeout_errors()
            await send_error_response(
                code=504, message="Gateway Timeout", writer=self.writer
            )

        except Exception as e:  # noqa: BLE001
            self.metrics.increment_failed_total()
            logger.info(f"Получение заголовков: {e}, from_client={from_client}")

    async def get_and_parse_headers(
        self, reader: StreamReader, from_client: bool = True
    ) -> tuple[bytes | None, dict[str, str] | None]:
        headers = await self.get_headers(reader=reader, from_client=from_client)
        if not headers:
            # значит мы перехватили исключение и уже написали об этом в логах
            return None, None
        header_lines = headers.split(b"\r\n")
        if not header_lines:
            logger.info("Пустые заголовки")
            return None, None
        first_line_header = header_lines[0].decode(errors="replace")
        try:
            one, two, three = first_line_header.split(" ")
            logger_msg = (
                "method %s, path %s, version %s"
                if from_client
                else "upstream version %s, status %s %s"
            )
            logger.info(logger_msg, one, two, three)
        except ValueError:
            logger.info("Ошибка получения первой строки заголовков")
            self.metrics.increment_failed_total()
            return None, None
        headers_map = dict(first_line=(one, two, three))
        for line in header_lines[1:]:
            try:
                str_line = line.decode(errors="replace")
                if ":" not in str_line:
                    continue
                key, value = str_line.split(":", 1)
                if key.strip().lower() in [
                    "content-length",
                    "transfer-encoding",
                    "connection",
                ]:
                    headers_map[key.strip().lower()] = value.strip().lower()
            except Exception as e:  # noqa: BLE001
                self.metrics.increment_failed_total()
                logger.error(e)
        return (headers, headers_map)

    async def get_target_reader_writer(self, host: str, port: int):
        try:
            return await asyncio.wait_for(
                up_pool.get_connection(peer_port=self.address[1], host=host, port=port),
                timeout=config.timeouts.connect_ms,
            )
        except OpenConnError as e:
            logger.error(e)
            await send_error_response(
                code=502, message="Bad Gateway", writer=self.writer
            )
            self.metrics.increment_failed_total()
            return None, None
        except TimeoutError as e:
            logger.error(e)
            await send_error_response(
                code=504, message="Gateway Timeout", writer=self.writer
            )
            self.metrics.increment_failed_total()
            self.metrics.increment_refused_by_limit_total()
            return None, None
        except Exception as e:
            logger.error(e)
            await send_error_response(
                code=500, message="Internal Server Error", writer=self.writer
            )
            self.metrics.increment_failed_total()
            return None, None

    async def proxy_data(self, tasks: list) -> tuple[bool, bool]:
        try:
            request_ok, (response_ok, upstr_keep_alive) = await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=config.timeouts.total_ms
            )

            return request_ok and response_ok, upstr_keep_alive

        except TimeoutError:
            logger.error("Ошибка прокси: Timeout")
            self.metrics.increment_failed_total()
            self.metrics.increment_timeout_errors()
            await send_error_response(
                code=504, message="Gateway Timeout", writer=self.writer
            )
            return False, False
        except Exception as e:
            logger.error(f"Ошибка прокси: {e}")
            self.metrics.increment_failed_total()
            await send_error_response(
                code=500, message="Internal Server Error", writer=self.writer
            )
            return False, False

    async def handle_client(self):
        datetime_started = datetime.now(UTC)
        # Не очень понимаю как считать время до получения апстрима внутри цикла,
        # поэтому записываю только время до апсрима при первом запросе от клиента
        seconds_to_upstream = 0

        while not self.reader.at_eof() and not self.writer.is_closing():
            headers, headers_map = await self.get_and_parse_headers(reader=self.reader)

            if not headers:
                break
            try:
                async with self.semaphore:
                    uhost, uport = await up_pool.get_upstream()

                    upstr_semaphore = up_pool.upstream_semaphores.get((uhost, uport))
                    async with upstr_semaphore:
                        self.metrics.increment_active_clients_by_upstream(
                            (uhost, uport)
                        )
                        (
                            target_reader,
                            target_writer,
                        ) = await self.get_target_reader_writer(uhost, uport)
                        if not all([target_reader, target_writer]):
                            break
                        if datetime_started:
                            seconds_to_upstream = (
                                datetime.now(UTC) - datetime_started
                            ).total_seconds()
                            datetime_started = None
                        self.metrics.update_upstream_and_time(
                            upstream_key=(uhost, uport),
                            time_to_connect=seconds_to_upstream,
                        )
                        self.metrics.increment_requests_total()

                        tasks = [
                            asyncio.create_task(
                                self.relay_request_or_response(
                                    headers=headers,
                                    headers_map=headers_map,
                                    writer=target_writer,
                                    reader=self.reader,
                                )
                            ),
                            asyncio.create_task(
                                self.upstream_data(target_reader=target_reader)
                            ),
                        ]

                        success, upstr_keep_alive = await self.proxy_data(tasks=tasks)
                        await up_pool.release_connection(
                            (uhost, uport),
                            target_reader,
                            target_writer,
                            upstr_keep_alive,
                        )
                        self.metrics.decrement_active_clients_by_upstream(
                            (uhost, uport)
                        )

                    if not success or headers_map.get("connection") == "close":
                        self.writer.close()
                        await self.writer.wait_closed()

            except TimeoutError as e:
                logger.error(e)
                await send_error_response(
                    code=504, message="Gateway Timeout", writer=self.writer
                )
                self.metrics.increment_failed_total()
                self.metrics.increment_refused_by_limit_total()
                success = False
                upstr_keep_alive = False

            except Exception as e:
                logger.error(e)
                await send_error_response(
                    code=500, message="Internal Server Error", writer=self.writer
                )
                self.metrics.increment_failed_total()
                success = False
                upstr_keep_alive = False

        self.writer.close()
        await self.writer.wait_closed()

    async def relay_request_or_response(
        self,
        headers: bytes,
        headers_map: dict[str, str],
        writer: StreamWriter,
        reader: StreamReader,
    ) -> bool:
        """Отправка запроса к upstream серверу или получение ответа от него"""
        if not await asyncio.wait_for(
            self.send_headers(headers=headers, writer=writer),
            timeout=config.timeouts.write_ms,
        ):
            return False
        if headers_map.get("first_line")[0] in ["GET", "HEAD", "OPTIONS"]:
            # запрос от клиента, тело отсутствует
            return True
        if headers_map.get("transfer-encoding") == "chunked":
            return await asyncio.wait_for(
                self.stream_chunked_body(reader=reader, writer=writer),
                timeout=config.timeouts.write_ms,
            )
        if content_length := headers_map.get("content-length"):
            return await asyncio.wait_for(
                self.stream_body(
                    reader=reader,
                    writer=writer,
                    content_length=int(content_length),
                ),
                timeout=config.timeouts.write_ms,
            )

        return False

    async def upstream_data(self, target_reader: StreamReader) -> tuple[bool, bool]:
        """Получение ответа от upstream сервера"""
        # т.к внутри получения заголовков уже есть wait_for,  не добавляем таймаут
        upstr_headers, upstr_headers_map = await self.get_and_parse_headers(
            reader=target_reader, from_client=False
        )

        if not upstr_headers:
            return False, False
        if await self.relay_request_or_response(
            headers=upstr_headers,
            headers_map=upstr_headers_map,
            writer=self.writer,
            reader=target_reader,
        ):
            return True, upstr_headers_map.get("connection") != "close"
        return False, False

    async def stream_body(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        content_length: int,
    ) -> bool:
        try:
            while content_length and content_length > 0:
                chunk = await reader.read(min(1024, content_length))
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
                content_length -= len(chunk)

            return True
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.error(f"send_body failed: {e}")
            return False

    async def stream_chunked_body(
        self, reader: StreamReader, writer: StreamWriter
    ) -> bool:
        try:
            while True:
                chunk_size_line = await reader.readuntil(b"\r\n")
                writer.write(chunk_size_line)
                await writer.drain()
                if not chunk_size_line:
                    return False
                try:
                    chunk_size = int(chunk_size_line.decode().strip("\r\n"), 16)
                except ValueError:
                    return False
                if chunk_size != 0:
                    body_line = await reader.readuntil(b"\r\n")
                    writer.write(body_line)
                    await writer.drain()
                else:
                    tail_line = await reader.readuntil(b"\r\n")
                    writer.write(tail_line)
                    await writer.drain()
                    return tail_line == b"\r\n"
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.error(f"stream_chunked_body failed: {e}")
            return False

    async def send_headers(self, headers: bytes, writer: StreamWriter) -> bool:
        try:
            writer.write(headers)
            await writer.drain()
            return True
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.error(f"send_headers failed: {e}")
            return False
