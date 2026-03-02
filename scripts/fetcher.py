"""
CDT - Compliance Digital Twin
Fetcher v4 — Fixes: IDs estables, seen_ids separado, umbral permisivo
"""

import os
import re
import json
import time
import hashlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
MISTRAL_API_KEY     = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_MODEL       = os.environ.get("MISTRAL_MODEL", "mistral-small-latest")
NEWSAPI_KEY         = os.environ.get("NEWSAPI_KEY", "")
RELEVANCE_THRESHOLD = int(os.environ.get("RELEVANCE_THRESHOLD", "35"))  # FIX: bajado de 60 a 35

DATA_DIR   = Path(__file__).parent.parent / "data"
HITS_FILE  = DATA_DIR / "hits.json"
SEEN_FILE  = DATA_DIR / "seen_ids.json"   # FIX: archivo separado de IDs vistos
CONFIG_FILE = DATA_DIR / "config.json"
DATA_DIR.mkdir(exist_ok=True)

MAX_HITS_STORED = 500   # FIX: subido de 200 a 500
MAX_SEEN_IDS    = 10000 # Máximo de IDs a recordar en seen_ids.json

# ─────────────────────────────────────────────
# CONFIGURACIÓN POR DEFECTO
# ─────────────────────────────────────────────
DEFAULT_CONFIG = {
    "newsapi_queries": [
        '(multa OR sanción OR "expediente sancionador") AND (empresa OR sociedad) AND España',
        '(CNMC OR AEPD OR CNMV OR "Banco de España" OR SEPI) AND (resolución OR sanción OR investigación)',
        '("compliance" OR "cumplimiento normativo" OR "canal de denuncias" OR "blanqueo de capitales" OR "corrupción empresarial") AND España',
        '("responsabilidad penal" OR "persona jurídica" OR "delito corporativo" OR "fraude empresarial") AND (sentencia OR condena OR imputado)',
        '("protección de datos" OR RGPD OR "brecha de seguridad") AND (sanción OR multa OR AEPD)',
    ],
    "newsapi_sources": "",
    "custom_keywords": [
        "compliance", "multa", "sanción", "corrupción", "blanqueo",
        "RGPD", "CNMC", "AEPD", "CNMV", "fraude", "soborno",
    ],
    "rss_enabled": {
        "BOE_disposiciones":        True,
        "BOE_justicia":             True,
        "BOE_anuncios":             True,
        "BOE_derecho_penal":        True,
        "BOE_derecho_mercantil":    True,
        "BOE_sistema_financiero":   True,
        "BOE_tribunal_constitucional": True,
        "BORME_general":            True,
    }
}

# ─────────────────────────────────────────────
# FUENTES RSS
# ─────────────────────────────────────────────
RSS_SOURCES = {
    "BOE_disposiciones":         {"url": "https://www.boe.es/rss/boe.php?s=1",          "name": "BOE - Disposiciones generales"},
    "BOE_justicia":              {"url": "https://www.boe.es/rss/boe.php?s=4",          "name": "BOE - Administración de Justicia"},
    "BOE_anuncios":              {"url": "https://www.boe.es/rss/boe.php?s=5B",         "name": "BOE - Anuncios oficiales"},
    "BOE_derecho_penal":         {"url": "https://www.boe.es/rss/canal_leg.php?l=l&c=113", "name": "BOE - Legislación Derecho Penal"},
    "BOE_derecho_mercantil":     {"url": "https://www.boe.es/rss/canal_leg.php?l=l&c=112", "name": "BOE - Legislación Derecho Mercantil"},
    "BOE_sistema_financiero":    {"url": "https://www.boe.es/rss/canal_leg.php?l=l&c=127", "name": "BOE - Sistema Financiero"},
    "BOE_tribunal_constitucional": {"url": "https://www.boe.es/rss/canal.php?c=tc",    "name": "BOE - Tribunal Constitucional"},
    "BORME_general":             {"url": "https://www.boe.es/rss/borme.php",            "name": "BORME - Registro Mercantil"},
}

BASE_KEYWORDS = [
    "corrupción", "soborno", "cohecho", "malversación", "blanqueo",
    "financiación del terrorismo", "multa", "sanción", "infracción grave",
    "compliance", "cumplimiento normativo", "due diligence",
    "protección de datos", "RGPD", "brecha de seguridad",
    "prácticas anticompetitivas", "insider trading", "información privilegiada",
    "responsabilidad penal", "persona jurídica", "delito empresarial",
    "acoso laboral", "acoso sexual", "greenwashing", "fraude",
]


# ─────────────────────────────────────────────
# HELPERS — IDs ESTABLES
# ─────────────────────────────────────────────
def normalize_title(title: str) -> str:
    """Normaliza un título para comparación robusta."""
    t = title.strip().lower()
    t = re.sub(r'\s+', ' ', t)
    # Eliminar puntuación final variable
    t = re.sub(r'[.,;:!?]+$', '', t)
    return t


def article_id(article: dict) -> str:
    """
    FIX v4: ID basado en título normalizado, no en URL.
    La URL del BOE puede tener parámetros variables entre ejecuciones;
    el título es estable y suficientemente único para deduplicar.
    Si hay link, lo añadimos como tiebreaker solo si el título es muy corto.
    """
    title = normalize_title(article.get("title", ""))
    if len(title) < 20 and article.get("link"):
        # Título muy corto — añadir dominio del link para diferenciar
        link = article.get("link", "")
        domain = re.sub(r'https?://(www\.)?', '', link).split('/')[0]
        raw = (title + domain).encode()
    else:
        raw = title.encode()
    return hashlib.md5(raw).hexdigest()[:12]


# ─────────────────────────────────────────────
# PERSISTENCIA — SEEN IDS (FIX v4)
# ─────────────────────────────────────────────
def load_seen_ids() -> set:
    """
    FIX v4: Carga el histórico completo de IDs ya procesados.
    Independiente del límite de hits almacenados — nunca perdemos
    la memoria de qué artículos ya vimos.
    """
    if SEEN_FILE.exists():
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    # Fallback: cargar IDs desde hits.json existente (migración desde v3)
    stored = load_existing_hits()
    return {h.get("id") for h in stored.get("hits", []) if h.get("id")}


def save_seen_ids(seen: set):
    """Persiste el set de IDs vistos, manteniendo un máximo para no crecer indefinidamente."""
    ids_list = list(seen)
    if len(ids_list) > MAX_SEEN_IDS:
        # Conservar los más recientes (asumiendo orden de inserción aproximado)
        ids_list = ids_list[-MAX_SEEN_IDS:]
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(ids_list, f)


# ─────────────────────────────────────────────
# CARGA DE CONFIGURACIÓN
# ─────────────────────────────────────────────
def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                return {**DEFAULT_CONFIG, **saved}
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()


# ─────────────────────────────────────────────
# INGESTA
# ─────────────────────────────────────────────
def clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    for ent, rep in [("&amp;","&"),("&lt;","<"),("&gt;",">"),("&nbsp;"," "),("&#39;","'"),("&quot;",'"')]:
        text = text.replace(ent, rep)
    return re.sub(r"\s+", " ", text).strip()


def fetch_rss(url: str, source_name: str) -> list[dict]:
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; CDT-ComplianceBot/1.0)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()

        if "html" in resp.headers.get("Content-Type", "") and b"<rss" not in resp.content[:500]:
            print(f"  [{source_name}] Respuesta HTML inesperada")
            return []

        root  = ET.fromstring(resp.content)
        items = root.findall(".//item")
        print(f"  [{source_name}] {len(items)} artículos")

        for item in items[:25]:
            title       = (item.findtext("title") or "").strip()
            link        = (item.findtext("link") or "").strip()
            description = clean_html((item.findtext("description") or "").strip())
            pub_date    = (item.findtext("pubDate") or "").strip()
            if title:
                articles.append({
                    "title":       title,
                    "link":        link,
                    "description": description[:600],
                    "date":        pub_date or datetime.now(timezone.utc).isoformat(),
                    "source":      source_name,
                })
    except requests.RequestException as e:
        print(f"  [{source_name}] Error red: {e}")
    except ET.ParseError as e:
        print(f"  [{source_name}] Error XML: {e}")
    except Exception as e:
        print(f"  [{source_name}] Error: {e}")
    return articles


def fetch_newsapi(query: str, sources: str = "", label: str = "NewsAPI") -> list[dict]:
    if not NEWSAPI_KEY:
        return []
    articles = []
    try:
        params = {
            "q":        query,
            "language": "es",
            "sortBy":   "publishedAt",
            "pageSize": 15,
            "apiKey":   NEWSAPI_KEY,
        }
        if sources:
            params["sources"] = sources

        resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for art in data.get("articles", []):
            title = art.get("title", "")
            desc  = art.get("description", "") or ""
            if title and title != "[Removed]":
                articles.append({
                    "title":       title,
                    "link":        art.get("url", ""),
                    "description": desc[:600],
                    "date":        art.get("publishedAt", datetime.now(timezone.utc).isoformat()),
                    "source":      f"{label} · {art.get('source',{}).get('name','?')}",
                })
        print(f"  [{label}] {len(articles)} artículos — query: {query[:60]}...")
    except Exception as e:
        print(f"  [{label}] Error: {e}")
    return articles


def is_relevant(article: dict, keywords: list[str]) -> bool:
    text = (article["title"] + " " + article["description"]).lower()
    all_kw = BASE_KEYWORDS + keywords
    return any(kw.lower() in text for kw in all_kw)


# ─────────────────────────────────────────────
# ANÁLISIS MISTRAL
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """Eres un experto en compliance corporativo español (ISO 37301, UNE 19601, ISO 37001, RGPD, normativa CNMV/AEPD/CNMC).

Analiza el evento y responde SOLO con este JSON exacto (sin markdown ni texto extra):
{
  "relevance_score": <0-100>,
  "level": "<critical|warning|info|irrelevant>",
  "risks": ["<riesgo1>", "<riesgo2>"],
  "norms_affected": ["<norma1>"],
  "summary": "<resumen en 2-3 frases>",
  "vulnerability": "<control que debería existir>",
  "financial_impact": "<estimación impacto económico en empresa española media>",
  "recommended_action": "<acción inmediata para el Compliance Officer>"
}

Niveles:
- critical: multa >500K€, condena penal, nuevo reglamento de obligado cumplimiento urgente
- warning: multa 50-500K€, expediente sancionador abierto, sentencia relevante
- info: guía/recomendación, jurisprudencia de interés moderado, multa <50K€
- irrelevant: no relacionado con compliance corporativo"""


def analyze_with_mistral(article: dict) -> dict | None:
    if not MISTRAL_API_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":   MISTRAL_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content":
                        f"Título: {article['title']}\n"
                        f"Fuente: {article['source']}\n"
                        f"Fecha: {article['date']}\n\n"
                        f"Descripción: {article['description']}\n\n"
                        f"Enlace: {article['link']}"},
                ],
                "temperature":     0.1,
                "max_tokens":      600,
                "response_format": {"type": "json_object"},
            },
            timeout=30,
        )
        resp.raise_for_status()
        analysis = json.loads(resp.json()["choices"][0]["message"]["content"])
        if "relevance_score" not in analysis or "level" not in analysis:
            return None
        return analysis
    except Exception as e:
        print(f"    Mistral error: {e}")
        return None


# ─────────────────────────────────────────────
# PERSISTENCIA — HITS
# ─────────────────────────────────────────────
def load_existing_hits() -> dict:
    if HITS_FILE.exists():
        try:
            with open(HITS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"hits": [], "last_updated": None, "stats": {}}


def save_hits(data: dict):
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(HITS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ Guardado: {HITS_FILE}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"CDT Fetcher v4 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    cfg      = load_config()
    stored   = load_existing_hits()

    # FIX v4: usar seen_ids.json en lugar de IDs del hits.json truncado
    seen_ids = load_seen_ids()
    print(f"\n📚 IDs históricos conocidos: {len(seen_ids)}")

    all_articles = []
    new_hits     = []
    processed    = 0

    # ── RSS ───────────────────────────────────
    print("\n📡 RSS feeds...")
    rss_enabled = cfg.get("rss_enabled", {})
    for key, source in RSS_SOURCES.items():
        if rss_enabled.get(key, True):
            all_articles.extend(fetch_rss(source["url"], source["name"]))
            time.sleep(1.5)

    # ── NEWSAPI ───────────────────────────────
    if NEWSAPI_KEY:
        print("\n📰 NewsAPI...")
        sources_filter = cfg.get("newsapi_sources", "")
        for i, query in enumerate(cfg.get("newsapi_queries", [])):
            all_articles.extend(
                fetch_newsapi(query, sources_filter, f"NewsAPI-Q{i+1}")
            )
            time.sleep(1)

    # ── FILTRO DE KEYWORDS ────────────────────
    custom_kw = cfg.get("custom_keywords", [])
    print(f"\n🔍 Filtrando {len(all_articles)} artículos...")
    relevant  = [a for a in all_articles if is_relevant(a, custom_kw)]

    # Deduplicar usando IDs estables + seen_ids histórico
    new_articles = []
    seen_this_run = set()
    duplicates    = 0
    for art in relevant:
        aid = article_id(art)
        art["id"] = aid
        if aid in seen_ids or aid in seen_this_run:
            duplicates += 1
            continue
        new_articles.append(art)
        seen_this_run.add(aid)

    print(f"   → {len(relevant)} relevantes | {duplicates} ya vistos | {len(new_articles)} nuevos a analizar")

    # ── ANÁLISIS MISTRAL ──────────────────────
    if new_articles and MISTRAL_API_KEY:
        batch = new_articles[:30]
        print(f"\n🤖 Analizando {len(batch)} artículos (umbral: {RELEVANCE_THRESHOLD})...")
        for i, article in enumerate(batch):
            print(f"  [{i+1}/{len(batch)}] {article['title'][:70]}...")
            analysis  = analyze_with_mistral(article)
            processed += 1

            # FIX v4: marcar como visto SIEMPRE, incluso si se descarta
            # Así no re-analizamos el mismo artículo en la próxima ejecución
            seen_ids.add(article["id"])

            if not analysis:
                print(f"    → Error de análisis, marcado como visto")
                continue

            level = analysis.get("level", "info")
            score = analysis.get("relevance_score", 0)

            if level == "irrelevant":
                print(f"    → Irrelevante (descartado)")
                continue

            # FIX v4: umbral más permisivo (35 en lugar de 60)
            # El filtro fino lo hace el RAG de la app con contexto de empresa
            if score < RELEVANCE_THRESHOLD:
                print(f"    → Score {score} < umbral {RELEVANCE_THRESHOLD} (descartado)")
                continue

            new_hits.append({
                "id":                 article["id"],
                "title":              article["title"],
                "link":               article["link"],
                "source":             article["source"],
                "raw_date":           article["date"],
                "fetched_at":         datetime.now(timezone.utc).isoformat(),
                "level":              level,
                "relevance_score":    score,
                "risks":              analysis.get("risks", []),
                "norms_affected":     analysis.get("norms_affected", []),
                "summary":            analysis.get("summary", ""),
                "vulnerability":      analysis.get("vulnerability", ""),
                "financial_impact":   analysis.get("financial_impact", ""),
                "recommended_action": analysis.get("recommended_action", ""),
            })
            print(f"    ✓ [{level.upper()}] Score: {score}")
            time.sleep(1.2)

    elif not MISTRAL_API_KEY:
        print("\n⚠️  Sin Mistral — guardando artículos sin análisis IA")
        for art in new_articles[:30]:
            seen_ids.add(art["id"])
            new_hits.append({
                "id":              art["id"],
                "title":           art["title"],
                "link":            art["link"],
                "source":          art["source"],
                "raw_date":        art["date"],
                "fetched_at":      datetime.now(timezone.utc).isoformat(),
                "level":           "info",
                "relevance_score": 50,
                "summary":         art["description"][:300],
            })

    # ── GUARDAR ───────────────────────────────
    # FIX v4: límite subido a 500 hits
    all_hits = (new_hits + stored.get("hits", []))[:MAX_HITS_STORED]

    stats = {
        "total_hits":       len(all_hits),
        "critical_count":   sum(1 for h in all_hits if h.get("level") == "critical"),
        "warning_count":    sum(1 for h in all_hits if h.get("level") == "warning"),
        "info_count":       sum(1 for h in all_hits if h.get("level") == "info"),
        "new_this_run":     len(new_hits),
        "articles_checked": processed or len(new_articles),
        "seen_ids_total":   len(seen_ids),
        "sources_active":   [s["name"] for k, s in RSS_SOURCES.items() if rss_enabled.get(k, True)],
        "queries_active":   cfg.get("newsapi_queries", []),
    }

    save_hits({"hits": all_hits, "stats": stats, "last_updated": None})

    # FIX v4: persistir seen_ids después de cada ejecución
    save_seen_ids(seen_ids)
    print(f"✓ Guardado: {SEEN_FILE} ({len(seen_ids)} IDs)")

    print(f"\n{'='*60}")
    print(f"✅ {len(new_hits)} hits nuevos | Total: {len(all_hits)}")
    print(f"   Críticos: {stats['critical_count']} | Alertas: {stats['warning_count']} | Info: {stats['info_count']}")
    print(f"   IDs históricos: {len(seen_ids)} | Artículos analizados: {processed}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
