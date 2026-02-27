"""
CDT - Compliance Digital Twin
Fetcher: Lee fuentes de noticias y jurisprudencia, analiza con Mistral IA
y guarda los resultados en /data/hits.json

Fuentes:
- BOE (Boletín Oficial del Estado) - RSS gratuito
- CENDOJ (jurisprudencia) - RSS gratuito
- CNMV (sanciones mercado valores) - RSS gratuito
- AEPD (sanciones protección de datos) - RSS gratuito
- NewsAPI - API gratuita (100 req/día)
"""

import os
import json
import time
import hashlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURACIÓN (desde variables de entorno GitHub)
# ─────────────────────────────────────────────
MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_MODEL   = os.environ.get("MISTRAL_MODEL", "mistral-small-latest")
NEWSAPI_KEY     = os.environ.get("NEWSAPI_KEY", "")
RELEVANCE_THRESHOLD = int(os.environ.get("RELEVANCE_THRESHOLD", "60"))

DATA_DIR  = Path(__file__).parent.parent / "data"
HITS_FILE = DATA_DIR / "hits.json"
DATA_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# FUENTES RSS
# ─────────────────────────────────────────────
RSS_SOURCES = {
    "BOE_general": {
        "url": "https://www.boe.es/rss/canal.php?s=boe",
        "name": "BOE - Disposiciones generales",
        "enabled": True,
    },
    "BOE_sanciones": {
        "url": "https://www.boe.es/rss/canal.php?s=boe&tipo=A",
        "name": "BOE - Anuncios y sanciones",
        "enabled": True,
    },
    "CENDOJ_penal": {
        "url": "https://www.poderjudicial.es/search/rss.jsp?tipo_resolucion=&organo=&inicio=0&num_resolucion=&cendoj=&query=compliance+corrupcion+blanqueo+anticorrupcion&country=ES",
        "name": "CENDOJ - Sentencias compliance/corrupción",
        "enabled": True,
    },
    "CENDOJ_admin": {
        "url": "https://www.poderjudicial.es/search/rss.jsp?tipo_resolucion=&organo=&inicio=0&num_resolucion=&cendoj=&query=sancion+empresa+multa+infraccion&country=ES",
        "name": "CENDOJ - Sentencias sanciones empresariales",
        "enabled": True,
    },
    "CNMV_sanciones": {
        "url": "https://www.cnmv.es/portal/alfresco/d/d/workspace/SpacesStore/cnmv-rss-sanciones.xml",
        "name": "CNMV - Sanciones mercado de valores",
        "enabled": True,
    },
    "AEPD_resoluciones": {
        "url": "https://www.aepd.es/es/rss/resoluciones",
        "name": "AEPD - Resoluciones protección de datos",
        "enabled": True,
    },
    "AEPD_noticias": {
        "url": "https://www.aepd.es/es/rss/noticias",
        "name": "AEPD - Noticias",
        "enabled": True,
    },
}

# ─────────────────────────────────────────────
# KEYWORDS DE COMPLIANCE (para filtro previo)
# ─────────────────────────────────────────────
COMPLIANCE_KEYWORDS = [
    # Anticorrupción
    "corrupción", "soborno", "cohecho", "comisión ilícita", "tráfico de influencias",
    "enriquecimiento ilícito", "malversación", "prevaricación",
    # Blanqueo
    "blanqueo", "lavado de dinero", "financiación del terrorismo", "PBC", "AML",
    # Sanciones empresariales
    "multa", "sanción", "expediente sancionador", "infracción grave", "infracción muy grave",
    "resolución sancionadora",
    # Compliance corporativo
    "compliance", "cumplimiento normativo", "programa de cumplimiento", "canal de denuncias",
    "whistleblowing", "due diligence", "diligencia debida",
    # RGPD / datos
    "protección de datos", "RGPD", "LOPD", "brecha de seguridad", "transferencia internacional",
    "consentimiento", "datos personales",
    # Competencia
    "prácticas anticompetitivas", "cártel", "abuso de posición", "CNMC", "competencia desleal",
    # Laboral / igualdad
    "acoso laboral", "acoso sexual", "discriminación", "desigualdad retributiva", "brecha salarial",
    "plan de igualdad",
    # Medio ambiente / ESG
    "medioambiente", "ESG", "sostenibilidad", "greenwashing", "emisiones", "vertido",
    # Mercado de valores
    "insider trading", "información privilegiada", "manipulación de mercado", "abuso de mercado",
    # Penal empresarial
    "responsabilidad penal", "persona jurídica", "delito empresarial", "administrador concursal",
]


# ─────────────────────────────────────────────
# FUNCIONES DE INGESTA
# ─────────────────────────────────────────────

def fetch_rss(url: str, source_name: str) -> list[dict]:
    """Descarga y parsea un feed RSS. Devuelve lista de artículos."""
    articles = []
    try:
        headers = {
            "User-Agent": "CDT-ComplianceTwin/1.0 (compliance monitoring tool)",
            "Accept": "application/rss+xml, application/xml, text/xml"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        # Namespaces habituales en RSS
        ns = {
            "dc": "http://purl.org/dc/elements/1.1/",
            "content": "http://purl.org/rss/1.0/modules/content/",
        }

        items = root.findall(".//item")
        print(f"  [{source_name}] {len(items)} artículos encontrados")

        for item in items[:20]:  # Máximo 20 por fuente
            title       = item.findtext("title", "").strip()
            link        = item.findtext("link", "").strip()
            description = item.findtext("description", "").strip()
            pub_date    = item.findtext("pubDate", "").strip()
            dc_date     = item.findtext("dc:date", "", ns).strip()

            # Limpiar HTML del description
            description = clean_html(description)

            if title:
                articles.append({
                    "title":       title,
                    "link":        link,
                    "description": description[:500],
                    "date":        pub_date or dc_date or datetime.now(timezone.utc).isoformat(),
                    "source":      source_name,
                })

    except requests.RequestException as e:
        print(f"  [{source_name}] Error de red: {e}")
    except ET.ParseError as e:
        print(f"  [{source_name}] Error XML: {e}")

    return articles


def fetch_newsapi(query: str) -> list[dict]:
    """Busca noticias en NewsAPI (100 req/día en tier gratuito)."""
    if not NEWSAPI_KEY:
        print("  [NewsAPI] Sin API key, saltando...")
        return []

    articles = []
    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q":        query,
            "language": "es",
            "sortBy":   "publishedAt",
            "pageSize": 10,
            "apiKey":   NEWSAPI_KEY,
        }
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for art in data.get("articles", []):
            title       = art.get("title", "")
            description = art.get("description", "") or ""
            if title and title != "[Removed]":
                articles.append({
                    "title":       title,
                    "link":        art.get("url", ""),
                    "description": description[:500],
                    "date":        art.get("publishedAt", datetime.now(timezone.utc).isoformat()),
                    "source":      f"NewsAPI - {art.get('source', {}).get('name', 'Desconocido')}",
                })

        print(f"  [NewsAPI] {len(articles)} artículos encontrados")

    except Exception as e:
        print(f"  [NewsAPI] Error: {e}")

    return articles


def clean_html(text: str) -> str:
    """Elimina tags HTML básicos de un texto."""
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;",  "&",  text)
    text = re.sub(r"&lt;",   "<",  text)
    text = re.sub(r"&gt;",   ">",  text)
    text = re.sub(r"&nbsp;", " ",  text)
    text = re.sub(r"\s+",    " ",  text)
    return text.strip()


def is_relevant(article: dict) -> bool:
    """Filtro rápido: descarta artículos sin keywords de compliance."""
    text = (article["title"] + " " + article["description"]).lower()
    return any(kw.lower() in text for kw in COMPLIANCE_KEYWORDS)


def article_id(article: dict) -> str:
    """Genera un ID único por artículo basado en título + link."""
    raw = (article.get("title", "") + article.get("link", "")).encode()
    return hashlib.md5(raw).hexdigest()[:12]


# ─────────────────────────────────────────────
# ANÁLISIS CON MISTRAL
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """Eres un experto en compliance corporativo español especializado en:
- ISO 37301 (Sistemas de gestión de compliance)
- UNE 19601 (Compliance penal)
- ISO 37001 (Antisoborno)
- RGPD y LOPD-GDD
- Normativa CNMV, AEPD, CNMC
- Derecho penal empresarial español

Tu tarea es analizar noticias, resoluciones o sentencias y determinar su relevancia e impacto para empresas españolas.

Responde SIEMPRE en este formato JSON exacto (sin markdown, sin explicaciones fuera del JSON):
{
  "relevance_score": <número 0-100>,
  "level": "<critical|warning|info|irrelevant>",
  "risks": ["<riesgo1>", "<riesgo2>"],
  "norms_affected": ["<norma1>", "<norma2>"],
  "summary": "<resumen del evento en 2-3 frases>",
  "vulnerability": "<qué control de compliance debería existir para mitigar este riesgo>",
  "financial_impact": "<estimación del impacto económico potencial en empresa española media>",
  "recommended_action": "<acción inmediata que debería tomar un Compliance Officer>"
}

Criterios de nivel:
- critical: Multa >500K€, condena penal, escándalo reputacional grave, nueva obligación legal urgente
- warning: Multa 50K-500K€, expediente sancionador, riesgo alto identificado, sentencia relevante
- info: Multa <50K€, nueva guía/recomendación, jurisprudencia de interés moderado
- irrelevant: No relacionado con compliance corporativo
"""

def analyze_with_mistral(article: dict) -> dict | None:
    """Envía un artículo a Mistral y devuelve el análisis estructurado."""
    if not MISTRAL_API_KEY:
        print("  Sin API key de Mistral")
        return None

    prompt = f"""Título: {article['title']}

Fuente: {article['source']}
Fecha: {article['date']}

Descripción: {article['description']}

Enlace: {article['link']}

Analiza este evento desde la perspectiva del compliance corporativo español."""

    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {MISTRAL_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       MISTRAL_MODEL,
                "messages":    [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens":  600,
                "response_format": {"type": "json_object"},
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        analysis = json.loads(content)

        # Validar que tiene los campos mínimos
        if "relevance_score" not in analysis or "level" not in analysis:
            return None

        return analysis

    except requests.RequestException as e:
        print(f"    Error Mistral API: {e}")
    except json.JSONDecodeError as e:
        print(f"    Error JSON Mistral: {e}")
    except Exception as e:
        print(f"    Error inesperado: {e}")

    return None


# ─────────────────────────────────────────────
# GESTIÓN DE HITS (persistencia)
# ─────────────────────────────────────────────

def load_existing_hits() -> dict:
    """Carga los hits existentes del archivo JSON."""
    if HITS_FILE.exists():
        try:
            with open(HITS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"hits": [], "last_updated": None, "stats": {}}


def save_hits(data: dict):
    """Guarda los hits en el archivo JSON."""
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(HITS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Guardado: {HITS_FILE}")


def hits_to_id_set(hits: list) -> set:
    """Devuelve el conjunto de IDs ya procesados."""
    return {h.get("id") for h in hits}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"CDT Fetcher — {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Cargar hits previos
    stored = load_existing_hits()
    existing_ids = hits_to_id_set(stored.get("hits", []))
    new_hits = []
    processed = 0

    # ── 1. RSS FEEDS ──────────────────────────────────────────
    print("\n📡 Descargando RSS feeds...")
    all_articles = []

    for source_key, source_cfg in RSS_SOURCES.items():
        if not source_cfg.get("enabled", True):
            continue
        articles = fetch_rss(source_cfg["url"], source_cfg["name"])
        all_articles.extend(articles)
        time.sleep(1)  # Educado con los servidores

    # ── 2. NEWSAPI ────────────────────────────────────────────
    print("\n📰 Descargando NewsAPI...")
    newsapi_query = "multa compliance corrupción sanción empresa España"
    all_articles.extend(fetch_newsapi(newsapi_query))

    time.sleep(1)

    newsapi_query2 = "CNMC AEPD CNMV resolución sanción empresa"
    all_articles.extend(fetch_newsapi(newsapi_query2))

    # ── 3. FILTRO DE RELEVANCIA RÁPIDO ───────────────────────
    print(f"\n🔍 Filtrando {len(all_articles)} artículos por keywords...")
    relevant = [a for a in all_articles if is_relevant(a)]
    print(f"   → {len(relevant)} artículos relevantes")

    # Eliminar duplicados por ID
    new_articles = []
    for art in relevant:
        art_id = article_id(art)
        if art_id not in existing_ids:
            art["id"] = art_id
            new_articles.append(art)

    print(f"   → {len(new_articles)} artículos nuevos (no procesados antes)")

    # ── 4. ANÁLISIS CON MISTRAL ───────────────────────────────
    if new_articles and MISTRAL_API_KEY:
        print(f"\n🤖 Analizando con Mistral AI ({len(new_articles)} artículos)...")

        for i, article in enumerate(new_articles[:30]):  # Max 30 por ejecución
            print(f"  [{i+1}/{min(len(new_articles),30)}] {article['title'][:70]}...")

            analysis = analyze_with_mistral(article)
            processed += 1

            if analysis is None:
                continue

            # Descartar si no es relevante según la IA
            if analysis.get("level") == "irrelevant":
                print(f"    → Descartado por IA (irrelevante)")
                continue

            score = analysis.get("relevance_score", 0)
            if score < RELEVANCE_THRESHOLD:
                print(f"    → Score {score} < umbral {RELEVANCE_THRESHOLD}, descartado")
                continue

            hit = {
                "id":                  article["id"],
                "title":               article["title"],
                "link":                article["link"],
                "source":              article["source"],
                "raw_date":            article["date"],
                "fetched_at":          datetime.now(timezone.utc).isoformat(),
                "level":               analysis.get("level", "info"),
                "relevance_score":     analysis.get("relevance_score", 0),
                "risks":               analysis.get("risks", []),
                "norms_affected":      analysis.get("norms_affected", []),
                "summary":             analysis.get("summary", ""),
                "vulnerability":       analysis.get("vulnerability", ""),
                "financial_impact":    analysis.get("financial_impact", ""),
                "recommended_action":  analysis.get("recommended_action", ""),
            }

            new_hits.append(hit)
            print(f"    ✓ [{analysis.get('level','?').upper()}] Score: {score}")

            # Respetar rate limit de Mistral (free tier: ~1 req/seg)
            time.sleep(1.2)

    elif not MISTRAL_API_KEY:
        print("\n⚠️  Sin API key de Mistral — los artículos no serán analizados")
    else:
        print("\n✓ Sin artículos nuevos que analizar")

    # ── 5. GUARDAR ────────────────────────────────────────────
    print(f"\n💾 Guardando resultados...")

    # Combinar hits nuevos con existentes (los nuevos primero)
    all_hits = new_hits + stored.get("hits", [])

    # Mantener máximo 200 hits (los más recientes)
    all_hits = all_hits[:200]

    stats = {
        "total_hits":       len(all_hits),
        "critical_count":   sum(1 for h in all_hits if h.get("level") == "critical"),
        "warning_count":    sum(1 for h in all_hits if h.get("level") == "warning"),
        "info_count":       sum(1 for h in all_hits if h.get("level") == "info"),
        "new_this_run":     len(new_hits),
        "articles_checked": processed,
    }

    save_hits({"hits": all_hits, "stats": stats, "last_updated": None})

    print(f"\n{'='*60}")
    print(f"✅ Completado:")
    print(f"   Artículos revisados: {processed}")
    print(f"   Hits nuevos añadidos: {len(new_hits)}")
    print(f"   Total hits en base: {len(all_hits)}")
    print(f"   Críticos: {stats['critical_count']} | Alertas: {stats['warning_count']} | Info: {stats['info_count']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
