import asyncio
from asyncio.streams import StreamReader, StreamWriter
from datetime import UTC, datetime

from errors import OpenConnError, SemaphoreTimeOutError, send_error_response
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

        except Exception as e:  # noqa: BLE001
            logger.info(f"Получение заголовков: {e}, from_client={from_client}")

    async def get_and_parse_headers(
        self, reader: StreamReader, from_client: bool = True
    ) -> tuple[bytes | None, bool | None, int | None]:
        headers = await self.get_headers(reader=reader, from_client=from_client)
        if not headers:
            # значит мы перехватили исключение и уже написали об этом в логах
            return None, None, None
        header_lines = headers.split(b"\r\n")
        if not header_lines:
            logger.info("Пустые заголовки")
            return None, None, None
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
            return None, None, None
        headers_map = {}
        for line in header_lines[1:]:
            try:
                str_line = line.decode(errors="replace")
                if ":" not in str_line:
                    continue
                key, value = str_line.split(":", 1)
                headers_map[key.strip().lower()] = value.strip()
            except Exception as e:  # noqa: BLE001
                logger.error(e)
        return (
            headers,
            # считаем,что если не передан заголовок connection равный close,
            # то соединение keep-alive
            headers_map.get("connection") != "close",
            int(headers_map.get("content-length", 0)),
        )

    async def get_target_reader_writer(self):
        try:
            return await up_pool.get_connection(peer_port=self.address[1])
        except OpenConnError as e:
            logger.error(e)
            await send_error_response(
                code=502, message="Bad Gateway", writer=self.writer
            )
            self.metrics.increment_failed_total()
            return None, None, None
        except SemaphoreTimeOutError as e:
            logger.error(e)
            await send_error_response(
                code=504, message="Gateway Timeout", writer=self.writer
            )
            self.metrics.increment_failed_total()
            self.metrics.increment_refused_by_limit_total()
            return None, None, None

    async def proxy_data(self, tasks: list, keep_alive: bool):
        try:
            request_ok, response_ok = await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=config.timeouts.total_ms
            )
            if not all([request_ok, response_ok]) and keep_alive:
                self.writer.close()
                await self.writer.wait_closed()
            return request_ok and response_ok

        except asyncio.TimeoutError:
            logger.error("Ошибка прокси: Timeout")
            self.metrics.increment_failed_total()
            self.metrics.increment_timeout_errors()
            await send_error_response(
                code=504, message="Gateway Timeout", writer=self.writer
            )
            return False
        except Exception as e:
            logger.error(f"Ошибка прокси: {e}")
            self.metrics.increment_failed_total()
            await send_error_response(
                code=500, message="Internal Server Error", writer=self.writer
            )
            return False

    async def handle_client(self):

        datetime_started = datetime.now(UTC)
        # Не очень понимаю как считать время до получения апстрима внутри цикла,
        # поэтому записываю только время до апсрима при первом запросе от клиента
        seconds_to_upstream = 0

        while not self.reader.at_eof() and not self.writer.is_closing():
            headers, keep_alive, content_length = await self.get_and_parse_headers(
                reader=self.reader
            )

            if not headers:
                break
            async with self.semaphore:
                (
                    target_reader,
                    target_writer,
                    upstream_address,
                ) = await self.get_target_reader_writer()
                if not all([target_reader, target_writer, upstream_address]):
                    break
                if datetime_started:
                    seconds_to_upstream = (
                        datetime.now(UTC) - datetime_started
                    ).total_seconds()
                    datetime_started = None
                self.metrics.update_upstream_and_time(
                    upstream_key=upstream_address,
                    time_to_connect=seconds_to_upstream,
                )
                self.metrics.increment_requests_total()

                tasks = [
                    asyncio.create_task(
                        self.relay_request(
                            headers=headers,
                            content_length=content_length,
                            writer=target_writer,
                        )
                    ),
                    asyncio.create_task(
                        self.relay_response(target_reader=target_reader)
                    ),
                ]
                success = await self.proxy_data(tasks=tasks, keep_alive=keep_alive)
                if not success:
                    break

            await up_pool.release_connection(
                upstream_address, target_reader, target_writer
            )

    async def relay_request(
        self,
        headers: bytes,
        content_length: int,
        writer: StreamWriter,
    ) -> bool:
        """Отправка запроса к upstream серверу"""
        if not await asyncio.wait_for(
            self.send_headers(headers=headers, writer=writer),
            timeout=config.timeouts.write_ms,
        ):
            return False
        if content_length:
            return await asyncio.wait_for(
                self.send_body(
                    reader=self.reader, writer=writer, content_length=content_length
                ),
                timeout=config.timeouts.write_ms,
            )
        return True

    async def relay_response(self, target_reader: StreamReader) -> bool:
        """Получение ответа от upstream сервера"""
        # т.к внутри получения заголовков уже есть wait_for,  не добавляем таймаут
        upstr_headers, _, u_content_length = await self.get_and_parse_headers(
            reader=target_reader, from_client=False
        )
        if not upstr_headers:
            return False
        if not await asyncio.wait_for(
            self.send_headers(headers=upstr_headers, writer=self.writer),
            timeout=config.timeouts.write_ms,
        ):
            return False
        return not u_content_length or await asyncio.wait_for(
            self.send_body(
                reader=target_reader,
                writer=self.writer,
                content_length=u_content_length,
            ),
            timeout=config.timeouts.write_ms,
        )

    async def send_body(
        self, reader: StreamReader, writer: StreamWriter, content_length: int
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
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"send_body failed: {e}")
            return False

    async def send_headers(self, headers: bytes, writer: StreamWriter) -> bool:
        try:
            writer.write(headers)
            await writer.drain()
            return True
        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"send_headers failed: {e}")
            return False
