/**
 * Vercel Serverless Function — Pinecone Proxy
 * Forwards requests from the browser to Pinecone API (bypasses CORS)
 * 
 * Deploy: put this file at /api/pinecone.js in your GitHub repo root
 * Env var: PINECONE_API_KEY in Vercel dashboard
 */

const PINECONE_HOST = 'https://digitaltwin-xm7jklu.svc.aped-4627-b74a.pinecone.io';

export default async function handler(req, res) {
  // CORS headers — allow requests from your Vercel app
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const apiKey = process.env.PINECONE_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'PINECONE_API_KEY not configured in Vercel environment variables' });
  }

  // Extract the Pinecone endpoint from query param: ?path=/records/upsert
  const path = req.query.path;
  if (!path) {
    return res.status(400).json({ error: 'Missing ?path= parameter' });
  }

  const url = PINECONE_HOST + path;

  try {
    const pcRes = await fetch(url, {
      method: req.method,
      headers: {
        'Api-Key': apiKey,
        'Content-Type': req.headers['content-type'] || 'application/json',
      },
      body: req.method !== 'GET' ? JSON.stringify(req.body) : undefined,
    });

    const data = await pcRes.text();

    // Forward Pinecone's status and response
    res.status(pcRes.status);
    res.setHeader('Content-Type', 'application/json');
    return res.send(data);

  } catch (err) {
    console.error('Pinecone proxy error:', err);
    return res.status(500).json({ error: err.message });
  }
}
