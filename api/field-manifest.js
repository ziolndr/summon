const DEFAULT_FIELD_ORIGIN =
  'https://89kzehob57fw.shares.zrok.io';

export default async function handler(request, response) {
  if (request.method !== 'GET') {
    response.setHeader('Allow', 'GET');
    return response.status(405).json({
      error: 'method not allowed'
    });
  }

  response.setHeader(
    'Cache-Control',
    'no-store, no-cache, must-revalidate, max-age=0'
  );

  const fieldOrigin =
    process.env.SUMMON_FIELD_ORIGIN ||
    DEFAULT_FIELD_ORIGIN;

  try {
    const upstream = await fetch(
      `${fieldOrigin}/field/v1/manifest`,
      {
        method: 'GET',
        cache: 'no-store',
        headers: {
          Accept: 'application/json'
        }
      }
    );

    const body = await upstream.text();

    if (!upstream.ok) {
      return response.status(502).json({
        error: 'live field manifest unavailable',
        upstream_status: upstream.status,
        upstream_body: body.slice(0, 500)
      });
    }

    const manifest = JSON.parse(body);
    const count = Number(manifest.count) || 0;

    if (count <= 0) {
      return response.status(502).json({
        error: 'live field manifest reported zero objects'
      });
    }

    response.setHeader(
      'X-SUMMON-Live-Objects',
      String(count)
    );

    return response.status(200).json(manifest);
  } catch (error) {
    return response.status(502).json({
      error: 'live field manifest proxy failed',
      detail: String(error)
    });
  }
}
