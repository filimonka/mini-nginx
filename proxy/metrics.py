from collections import defaultdict, deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

TIME_TO_CONNECT_WINDOW = 100


class Metrics:
    def __init__(self):
        self.requests_total: int = 0
        self.failed_total: int = 0
        self.refused_by_limit_total: int = 0
        self.active_clients_total: int = 0
        self.proxied_to_upstream: dict[tuple, int] = self._get_proxied_by_upstream()
        self.time_to_connect: deque[float] = deque(maxlen=TIME_TO_CONNECT_WINDOW)
        self.timeout_errors: int = 0
        self.new_clients_count: int = 0

    def increment_timeout_errors(self) -> None:
        self.timeout_errors += 1

    def _get_proxied_by_upstream(self):
        return defaultdict(int)

    def record_time_to_connect(self, seconds: float) -> None:
        self.time_to_connect.append(seconds)

    @property
    def avg_time_to_connect(self) -> float:
        if not self.time_to_connect:
            return 0.0
        return sum(self.time_to_connect) / len(self.time_to_connect)

    def increment_proxied_to_upstream(self, key: tuple) -> None:
        self.proxied_to_upstream[key] += 1

    def increment_new_clients_count(self) -> None:
        self.new_clients_count += 1

    def increment_active_client_count(self) -> None:
        self.active_clients_total += 1

    def decrement_active_client_count(self) -> None:
        self.active_clients_total -= 1

    def increment_requests_total(self) -> None:
        self.requests_total += 1

    def increment_failed_total(self) -> None:
        self.failed_total += 1

    def increment_refused_by_limit_total(self) -> None:
        self.refused_by_limit_total += 1

    def update_upstream_and_time(
        self, upstream_key: tuple, time_to_connect: float
    ) -> None:
        self.increment_proxied_to_upstream(upstream_key)
        self.record_time_to_connect(time_to_connect)


metrics = Metrics()

metrics_app = FastAPI()


@metrics_app.get("/metrics")
def show_metrics() -> dict:
    return HTMLResponse(
        content=f"""
        <html>
        <head><title>Статистика подключений</title></head>
        <body>
        <h1>Статистика</h1>
        <ul>
            <li>Всего запросов: {metrics.requests_total}</li>
        </ul>
        <ul>
            <li>Неудачных запросов: {metrics.failed_total}</li>
            <li>Ошибок Timeout: {metrics.timeout_errors}</li>
            <li>Отказано из-за лимита: {metrics.refused_by_limit_total}</li>
        </ul>
        <ul>
            <li>Активных клиентов: {metrics.active_clients_total}</li>
            <li>Всех уникальных клиентов: {metrics.new_clients_count}</li>
        </ul>
        <ul>
            <li>Проксировано на upstream: {dict(metrics.proxied_to_upstream)}</li>
            <li>Среднее время от подключения до проксирования: {metrics.avg_time_to_connect:.4f} секунд</li>
        </ul>
        </body>
        </html>
    """
    )
