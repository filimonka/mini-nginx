# mini-nginx

Локальный асинхронный HTTP-прокси с балансировкой запросов между upstream-серверами. Настройки адреса прокси, upstream’ов и лимитов находятся в [proxy/config.yml](proxy/config.yml).

## Требования

- Python 3.13 или новее
- [Poetry](https://python-poetry.org/docs/#installation)
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) — для нагрузочных тестов

Команды ниже рассчитаны на запуск из корня репозитория в Bash или Zsh.

## Установка

Перейдите в каталог проекта, выберите Python и установите зависимости:

```bash
cd mini-nginx
poetry env use python3.13
poetry install
```


## Запуск

Для локального тестирования нужны четыре отдельных терминала. Во всех командах предполагается, что текущий каталог — корень репозитория.

### 1. Upstream-серверы

Запустите по одному серверу в каждом из двух терминалов:

```bash
poetry run uvicorn tests.echo_app:app --host 127.0.0.1 --port 9001 --workers 1
```

```bash
poetry run uvicorn tests.echo_app:app --host 127.0.0.1 --port 9002 --workers 1
```

### 2. Прокси

В третьем терминале запустите прокси:

```bash
PYTHONPATH=proxy poetry run python -m proxy.main
```

Прокси будет слушать `http://127.0.0.1:8080`, а endpoint метрик — `http://127.0.0.1:8000`. Upstream-серверы и лимиты подключений задаются в [proxy/config.yml](proxy/config.yml).

### 3. Проверка запросов вручную

```bash
curl http://127.0.0.1:8080/anything
curl -X POST http://127.0.0.1:8080/something \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello"}'
```

Оба маршрута прокси перенаправляет на echo-серверы; ответы должны иметь HTTP-статус `200`.

## Тест k6

Запускайте тесты в четвёртом терминале, пока работают оба upstream’а и прокси. Перейдите в корень репозитория и запустите один из сценариев:

```bash
# Нагрузочный тест GET, POST, /stream и /echo с постепенным увеличением числа VU
k6 run tests/test_get.js

```

`test_get.js` на каждой итерации ходит в `/anything`, `/something`, `/stream` и `/echo`. Сценарий длится около 200 секунд и увеличивает нагрузку до 1500 виртуальных пользователей. На слабой машине тест может заметно нагрузить процессор и сеть. По завершении k6 выводит сводку длительности запросов и ошибок.

Чтобы остановить любой сервер, нажмите `Ctrl+C` в его терминале.
