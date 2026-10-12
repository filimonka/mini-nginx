// test_get.js - тест для вашего прокси с плавным наращиванием нагрузки,
// чтобы было видно, где именно начинается деградация.
// Текущая ёмкость апстримов: max_conns_per_upstream (100) * кол-во апстримов (2) = 200
// одновременных соединений к бэкендам (см. proxy/config.yml). Ступени ниже специально
// проходят через эту границу, чтобы деградация была видна на графиках/в summary.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const PROXY_URL = 'http://localhost:8080';

// Отдельные метрики на GET/POST, чтобы видеть деградацию по каждому типу запроса,
// а не усреднённую по всем.
const getDuration = new Trend('get_duration', true);
const postDuration = new Trend('post_duration', true);
const errorRate = new Rate('proxy_error_rate');
const badGatewayTotal = new Counter('proxy_502_bad_gateway');
const gatewayTimeoutTotal = new Counter('proxy_504_gateway_timeout');

export const options = {
    scenarios: {
        ramp_through_upstream_limit: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 50 },  // прогрев, заведомо ниже ёмкости апстримов
                { duration: '30s', target: 150 }, // всё ещё ниже ёмкости
                { duration: '30s', target: 200 }, // граница ёмкости апстримов
                { duration: '30s', target: 300 }, // выше ёмкости - ожидаем рост латентности/ошибок
                { duration: '30s', target: 500 }, // существенно выше ёмкости - явная деградация
                { duration: '30s', target: 1100 }, // max. разрешенных подключений к прокси
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
    errorRate.add(getRes.status !== 200);
    if (getRes.status === 502) badGatewayTotal.add(1);
    if (getRes.status === 504) gatewayTimeoutTotal.add(1);

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
    errorRate.add(postRes.status !== 200);
    if (postRes.status === 502) badGatewayTotal.add(1);
    if (postRes.status === 504) gatewayTimeoutTotal.add(1);

    check(postRes, {
        'POST: статус 200': (r) => r.status === 200,
        'POST: есть тело': (r) => r.body && r.body.length > 0,
    });

    sleep(0.1);
}
