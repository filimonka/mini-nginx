// test_get.js - тест для вашего прокси с плавным наращиванием нагрузки,
// чтобы было видно, где именно начинается деградация.
// Текущая ёмкость апстримов: max_conns_per_upstream (100) * кол-во апстримов (2) = 200
// одновременных соединений к бэкендам (см. proxy/config.yml). Ступени ниже специально
// проходят через эту границу, чтобы деградация была видна на графиках/в summary.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const PROXY_URL = 'http://localhost:8080';

// Отдельные метрики по типу запроса, чтобы видеть деградацию отдельно,
// а не усреднённую по всем.
const getDuration = new Trend('get_duration', true);
const postDuration = new Trend('post_duration', true);
const streamDuration = new Trend('stream_duration', true);
const echoDuration = new Trend('echo_duration', true);
const errorRate = new Rate('proxy_error_rate');
const badGatewayTotal = new Counter('proxy_502_bad_gateway');
const gatewayTimeoutTotal = new Counter('proxy_504_gateway_timeout');

function recordStatus(res) {
    errorRate.add(res.status !== 200);
    if (res.status === 502) badGatewayTotal.add(1);
    if (res.status === 504) gatewayTimeoutTotal.add(1);
}

export const options = {
    scenarios: {
        ramp_through_upstream_limit: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 150 },  // прогрев, заведомо ниже ёмкости апстримов
                { duration: '30s', target: 200 }, // граница ёмкости апстримов
                { duration: '30s', target: 400 }, // выше ёмкости - ожидаем рост латентности/ошибок
                { duration: '30s', target: 600 }, // выше ёмкости - ожидаем рост латентности/ошибок
                { duration: '30s', target: 800 }, // существенно выше ёмкости - явная деградация
                { duration: '30s', target: 1500 }, // max. разрешенных подключений к прокси
                { duration: '20s', target: 0 },   // плавный спуск
            ],
            gracefulRampDown: '10s',
        },
    },
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
    // Пороги не должны останавливать прогон - они лишь подсвечивают момент деградации.
    thresholds: {
        proxy_error_rate: [{ threshold: 'rate<1.0', abortOnFail: false }],
    },
};

export default function () {
    const getRes = http.get(`${PROXY_URL}/anything`, {
        headers: {
            'User-Agent': 'k6-test',
        },
        tags: { name: 'proxy_get' },
    });

    getDuration.add(getRes.timings.duration);
    recordStatus(getRes);

    check(getRes, {
        'GET: прокси отвечает 200': (r) => r.status === 200,
        'GET: ответ пришел': (r) => r.body && r.body.length > 0,
    });

    const payload = JSON.stringify({
        message: 'hello world',
    });

    const postRes = http.post(`${PROXY_URL}/something`, payload, {
        headers: {
            'Content-Type': 'application/json',
        },
        tags: { name: 'proxy_post' },
    });

    postDuration.add(postRes.timings.duration);
    recordStatus(postRes);

    check(postRes, {
        'POST: статус 200': (r) => r.status === 200,
        'POST: есть тело': (r) => r.body && r.body.length > 0,
    });

    const streamRes = http.get(`${PROXY_URL}/stream`, {
        headers: {
            'User-Agent': 'k6-test',
        },
        tags: { name: 'proxy_stream' },
    });

    streamDuration.add(streamRes.timings.duration);
    recordStatus(streamRes);

    check(streamRes, {
        'STREAM: статус 200': (r) => r.status === 200,
        'STREAM: пришли все части': (r) => r.body && r.body.includes('part 0') && r.body.includes('part 2'),
    });

    const echoPayload = 'echo payload';
    const echoRes = http.post(`${PROXY_URL}/echo`, echoPayload, {
        headers: {
            'Content-Type': 'text/plain',
        },
        tags: { name: 'proxy_echo' },
    });

    echoDuration.add(echoRes.timings.duration);
    recordStatus(echoRes);

    check(echoRes, {
        'ECHO: статус 200': (r) => r.status === 200,
        'ECHO: тело отражено': (r) => r.body && r.body.includes(echoPayload),
    });

    sleep(0.1);
}
