/**
 * Vercel Serverless Function — Pinecone Proxy
 * Handles embedding generation + vector upsert server-side (bypasses CORS)
 *
 * Endpoints:
 *   POST /api/pinecone?path=/upsert-text  → embed text then upsert vector
 *   POST /api/pinecone?path=/search-text  → embed query then search vectors
 *   GET  /api/pinecone?path=/describe_index_stats → forward directly
 *
 * Env var: PINECONE_API_KEY in Vercel dashboard
 */

const PC_INDEX_HOST = 'https://digitaltwin-xm7jklu.svc.aped-4627-b74a.pinecone.io';
const PC_API_HOST   = 'https://api.pinecone.io';
const PC_MODEL      = 'llama-text-embed-v2';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();

  const apiKey = process.env.PINECONE_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'PINECONE_API_KEY not set in Vercel env vars' });
  }

  const path = req.query.path || '';

  // ── GET /describe_index_stats ─────────────────────────────────────────────
  if (path === '/describe_index_stats') {
    const r = await fetch(PC_INDEX_HOST + '/describe_index_stats', {
      headers: { 'Api-Key': apiKey, 'Content-Type': 'application/json' }
    });
    const d = await r.text();
    return res.status(r.status).json(JSON.parse(d));
  }

  // ── POST /upsert-text  { id, text, metadata } ────────────────────────────
  // Embeds text then upserts the vector
  if (path === '/upsert-text') {
    const { id, text, metadata } = req.body;
    if (!id || !text) return res.status(400).json({ error: 'id and text required' });

    try {
      // 1. Generate embedding via Pinecone Inference API
      const embRes = await fetch(PC_API_HOST + '/embed', {
        method: 'POST',
        headers: {
          'Api-Key': apiKey,
          'Content-Type': 'application/json',
          'X-Pinecone-Api-Version': '2025-10'
        },
        body: JSON.stringify({
          model: PC_MODEL,
          inputs: [{ text: text.slice(0, 8000) }],
          parameters: { input_type: 'passage', truncate: 'END' }
        })
      });

      if (!embRes.ok) {
        const err = await embRes.text();
        console.error('Embed error:', embRes.status, err);
        return res.status(502).json({ error: 'Embedding failed', status: embRes.status, detail: err.slice(0, 300) });
      }

      const embData = await embRes.json();
      const values = embData.data?.[0]?.values;
      if (!values) return res.status(502).json({ error: 'No embedding values returned' });

      // 2. Upsert vector to index
      const upsertRes = await fetch(PC_INDEX_HOST + '/vectors/upsert', {
        method: 'POST',
        headers: { 'Api-Key': apiKey, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          vectors: [{ id, values, metadata: metadata || {} }]
        })
      });

      const upsertData = await upsertRes.text();
      return res.status(upsertRes.ok ? 200 : 502).json(
        upsertRes.ok ? { upserted: 1 } : { error: upsertData }
      );

    } catch (e) {
      console.error('upsert-text error:', e);
      return res.status(500).json({ error: e.message });
    }
  }

  // ── POST /search-text  { text, topK } ────────────────────────────────────
  // Embeds query then searches
  if (path === '/search-text') {
    const { text, topK = 8 } = req.body;
    if (!text) return res.status(400).json({ error: 'text required' });

    try {
      // 1. Embed query
      const embRes = await fetch(PC_API_HOST + '/embed', {
        method: 'POST',
        headers: {
          'Api-Key': apiKey,
          'Content-Type': 'application/json',
          'X-Pinecone-Api-Version': '2025-10'
        },
        body: JSON.stringify({
          model: PC_MODEL,
          inputs: [{ text: text.slice(0, 2000) }],
          parameters: { input_type: 'query', truncate: 'END' }
        })
      });

      if (!embRes.ok) {
        const err = await embRes.text();
        return res.status(502).json({ error: 'Embedding failed', detail: err });
      }

      const embData = await embRes.json();
      const vector = embData.data?.[0]?.values;
      if (!vector) return res.status(502).json({ error: 'No query vector returned' });

      // 2. Query index
      const queryRes = await fetch(PC_INDEX_HOST + '/query', {
        method: 'POST',
        headers: { 'Api-Key': apiKey, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          vector,
          topK,
          includeMetadata: true
        })
      });

      const queryData = await queryRes.json();
      return res.status(queryRes.ok ? 200 : 502).json(queryData);

    } catch (e) {
      console.error('search-text error:', e);
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(400).json({ error: 'Unknown path: ' + path });
}
