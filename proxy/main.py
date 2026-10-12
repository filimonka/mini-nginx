import asyncio

import uvicorn
from logger import logger
from metrics import metrics, metrics_app
from proxy_server import ProxyServer
from settings import config

HOST, PORT = config.get_host_port_to_listen()


async def server(host: str = HOST, port: int = PORT):
    tcp_server_start = ProxyServer(
        host=host,
        port=port,
        max_connections=config.limits.max_client_conns,
        metrics=metrics,
    ).start

    metrics_app_config = uvicorn.Config(
        metrics_app, host="127.0.0.1", port=8000, loop="asyncio"
    )
    metrics_app_server = uvicorn.Server(metrics_app_config)
    await asyncio.gather(
        tcp_server_start(),
        metrics_app_server.serve(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(server())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
