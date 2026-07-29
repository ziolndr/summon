const DEFAULT_FIELD_ORIGIN =
  'https://89kzehob57fw.shares.zrok.io';

function parseBody(request) {
  if (
    request.body &&
    typeof request.body === 'object' &&
    !Buffer.isBuffer(request.body)
  ) {
    return request.body;
  }

  if (Buffer.isBuffer(request.body)) {
    return JSON.parse(
      request.body.toString('utf8')
    );
  }

  if (typeof request.body === 'string') {
    return JSON.parse(request.body);
  }

  return {};
}

export default async function handler(request, response) {
  if (request.method === 'OPTIONS') {
    response.setHeader(
      'Allow',
      'POST, OPTIONS'
    );

    return response.status(204).end();
  }

  if (request.method !== 'POST') {
    response.setHeader(
      'Allow',
      'POST, OPTIONS'
    );

    return response.status(405).json({
      error: 'method not allowed'
    });
  }

  response.setHeader(
    'Cache-Control',
    'no-store, no-cache, must-revalidate, max-age=0'
  );

  let payload;

  try {
    payload = parseBody(request);
  } catch (error) {
    return response.status(400).json({
      error: 'invalid JSON body',
      detail: String(error)
    });
  }

  const text = String(
    payload.text || ''
  ).trim();

  if (!text) {
    return response.status(400).json({
      error: 'text is required'
    });
  }

  const k = Math.max(
    1,
    Math.min(
      100,
      Number(payload.k) || 50
    )
  );

  const offset = Math.max(
    0,
    Number(payload.offset) || 0
  );

  const forwardedPayload = {
    ...payload,
    text,
    k,
    offset
  };

  const fieldOrigin =
    process.env.SUMMON_FIELD_ORIGIN ||
    DEFAULT_FIELD_ORIGIN;

  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    55000
  );

  try {
    const upstream = await fetch(
      `${fieldOrigin}/field/v1/search`,
      {
        method: 'POST',
        cache: 'no-store',
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(
          forwardedPayload
        )
      }
    );

    const body = await upstream.text();

    response.status(upstream.status);

    response.setHeader(
      'Content-Type',
      upstream.headers.get('content-type') ||
      'application/json; charset=utf-8'
    );

    response.setHeader(
      'X-SUMMON-Search-Upstream',
      fieldOrigin
    );

    return response.send(body);
  } catch (error) {
    return response.status(502).json({
      error: 'live field search proxy failed',
      detail: String(error)
    });
  } finally {
    clearTimeout(timeout);
  }
}
