"""
CDT - Compliance Digital Twin
Fetcher v2 — URLs verificadas y corregidas
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
# CONFIGURACIÓN
# ─────────────────────────────────────────────
MISTRAL_API_KEY     = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_MODEL       = os.environ.get("MISTRAL_MODEL", "mistral-small-latest")
NEWSAPI_KEY         = os.environ.get("NEWSAPI_KEY", "")
RELEVANCE_THRESHOLD = int(os.environ.get("RELEVANCE_THRESHOLD", "60"))

DATA_DIR  = Path(__file__).parent.parent / "data"
HITS_FILE = DATA_DIR / "hits.json"
DATA_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# FUENTES RSS — URLs verificadas febrero 2026
# ─────────────────────────────────────────────
RSS_SOURCES = {
    # BOE — URLs correctas según boe.es/rss/
    "BOE_disposiciones": {
        "url": "https://www.boe.es/rss/boe.php?s=1",
        "name": "BOE - Sección I (Disposiciones generales)",
        "enabled": True,
    },
    "BOE_justicia": {
        "url": "https://www.boe.es/rss/boe.php?s=4",
        "name": "BOE - Sección IV (Administración de Justicia)",
        "enabled": True,
    },
    "BOE_anuncios": {
        "url": "https://www.boe.es/rss/boe.php?s=5B",
        "name": "BOE - Sección V.B (Anuncios oficiales)",
        "enabled": True,
    },
    "BOE_derecho_penal": {
        "url": "https://www.boe.es/rss/canal_leg.php?l=l&c=113",
        "name": "BOE - Legislación Derecho Penal",
        "enabled": True,
    },
    "BOE_derecho_mercantil": {
        "url": "https://www.boe.es/rss/canal_leg.php?l=l&c=112",
        "name": "BOE - Legislación Derecho Mercantil",
        "enabled": True,
    },
    "BOE_sistema_financiero": {
        "url": "https://www.boe.es/rss/canal_leg.php?l=l&c=127",
        "name": "BOE - Legislación Sistema Financiero",
        "enabled": True,
    },
    "BOE_tribunal_constitucional": {
        "url": "https://www.boe.es/rss/canal.php?c=tc",
        "name": "BOE - Sentencias Tribunal Constitucional",
        "enabled": True,
    },
    # BORME — Registro Mercantil
    "BORME_general": {
        "url": "https://www.boe.es/rss/borme.php",
        "name": "BORME - Registro Mercantil",
        "enabled": True,
    },
}

# ─────────────────────────────────────────────
# KEYWORDS DE COMPLIANCE
# ─────────────────────────────────────────────
COMPLIANCE_KEYWORDS = [
    "corrupción", "soborno", "cohecho", "comisión ilícita", "tráfico de influencias",
    "malversación", "prevaricación", "blanqueo", "lavado de dinero",
    "financiación del terrorismo", "multa", "sanción", "expediente sancionador",
    "infracción grave", "infracción muy grave", "resolución sancionadora",
    "compliance", "cumplimiento normativo", "programa de cumplimiento",
    "canal de denuncias", "whistleblowing", "due diligence", "diligencia debida",
    "protección de datos", "RGPD", "LOPD", "brecha de seguridad",
    "datos personales", "prácticas anticompetitivas", "cártel", "abuso de posición",
    "competencia desleal", "acoso laboral", "acoso sexual", "discriminación",
    "insider trading", "información privilegiada", "manipulación de mercado",
    "responsabilidad penal", "persona jurídica", "delito empresarial",
    "medioambiente", "ESG", "greenwashing", "vertido ilegal",
    "contratación pública", "licitación", "fraude", "falsedad documental",
    "administrador", "consejero", "directivo", "órgano de cumplimiento",
]


# ─────────────────────────────────────────────
# INGESTA RSS
# ─────────────────────────────────────────────
def fetch_rss(url: str, source_name: str) -> list[dict]:
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; CDT-ComplianceBot/1.0)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()

        # El BOE devuelve HTML cuando hay error — detectarlo
        content_type = resp.headers.get("Content-Type", "")
        if "html" in content_type and b"<rss" not in resp.content[:200]:
            print(f"  [{source_name}] Respuesta HTML inesperada (posible bloqueo)")
            return []

        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        print(f"  [{source_name}] {len(items)} artículos")

        for item in items[:25]:
            title       = (item.findtext("title") or "").strip()
            link        = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date    = (item.findtext("pubDate") or "").strip()

            description = clean_html(description)
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
        print(f"  [{source_name}] Error inesperado: {e}")

    return articles


def fetch_newsapi(query: str, label: str = "NewsAPI") -> list[dict]:
    if not NEWSAPI_KEY:
        return []
    articles = []
    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q":        query,
                "language": "es",
                "sortBy":   "publishedAt",
                "pageSize": 10,
                "apiKey":   NEWSAPI_KEY,
            },
            timeout=15,
        )
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
                    "source":      f"{label} - {art.get('source', {}).get('name', '?')}",
                })
        print(f"  [{label}] {len(articles)} artículos")
    except Exception as e:
        print(f"  [{label}] Error: {e}")
    return articles


def clean_html(text: str) -> str:
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    for ent, rep in [("&amp;","&"),("&lt;","<"),("&gt;",">"),("&nbsp;"," "),("&#39;","'")]:
        text = text.replace(ent, rep)
    return re.sub(r"\s+", " ", text).strip()


def is_relevant(article: dict) -> bool:
    text = (article["title"] + " " + article["description"]).lower()
    return any(kw.lower() in text for kw in COMPLIANCE_KEYWORDS)


def article_id(article: dict) -> str:
    raw = (article.get("title","") + article.get("link","")).encode()
    return hashlib.md5(raw).hexdigest()[:12]


# ─────────────────────────────────────────────
# ANÁLISIS MISTRAL
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """Eres un experto en compliance corporativo español (ISO 37301, UNE 19601, ISO 37001, RGPD, normativa CNMV/AEPD/CNMC).

Analiza el evento y responde SOLO con este JSON exacto (sin markdown):
{
  "relevance_score": <0-100>,
  "level": "<critical|warning|info|irrelevant>",
  "risks": ["<riesgo1>", "<riesgo2>"],
  "norms_affected": ["<norma1>"],
  "summary": "<resumen en 2-3 frases>",
  "vulnerability": "<control que debería existir>",
  "financial_impact": "<estimación impacto económico en empresa española>",
  "recommended_action": "<acción inmediata para el Compliance Officer>"
}

Niveles: critical=multa>500K€/condena penal | warning=multa 50-500K€/expediente | info=guía/jurisprudencia moderada | irrelevant=no relacionado"""


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
                "model":       MISTRAL_MODEL,
                "messages":    [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": f"Título: {article['title']}\nFuente: {article['source']}\nFecha: {article['date']}\n\nDescripción: {article['description']}\n\nEnlace: {article['link']}"},
                ],
                "temperature":   0.1,
                "max_tokens":    600,
                "response_format": {"type": "json_object"},
            },
            timeout=30,
        )
        resp.raise_for_status()
        content  = resp.json()["choices"][0]["message"]["content"]
        analysis = json.loads(content)
        if "relevance_score" not in analysis or "level" not in analysis:
            return None
        return analysis
    except Exception as e:
        print(f"    Mistral error: {e}")
        return None


# ─────────────────────────────────────────────
# PERSISTENCIA
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
    print(f"CDT Fetcher v2 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    stored       = load_existing_hits()
    existing_ids = {h.get("id") for h in stored.get("hits", [])}
    new_hits     = []
    processed    = 0

    # ── RSS FEEDS ─────────────────────────────
    print("\n📡 Descargando RSS feeds...")
    all_articles = []
    for key, cfg in RSS_SOURCES.items():
        if cfg.get("enabled", True):
            all_articles.extend(fetch_rss(cfg["url"], cfg["name"]))
            time.sleep(1.5)  # Educado con los servidores

    # ── NEWSAPI ───────────────────────────────
    if NEWSAPI_KEY:
        print("\n📰 Descargando NewsAPI...")
        all_articles.extend(fetch_newsapi("multa sanción compliance empresa España", "NewsAPI-Compliance"))
        time.sleep(1)
        all_articles.extend(fetch_newsapi("CNMC AEPD CNMV resolución sanción", "NewsAPI-Reguladores"))

    # ── FILTRO ────────────────────────────────
    print(f"\n🔍 Filtrando {len(all_articles)} artículos...")
    relevant     = [a for a in all_articles if is_relevant(a)]
    new_articles = []
    for art in relevant:
        aid = article_id(art)
        if aid not in existing_ids:
            art["id"] = aid
            new_articles.append(art)

    print(f"   → {len(relevant)} relevantes | {len(new_articles)} nuevos")

    # ── ANÁLISIS MISTRAL ──────────────────────
    if new_articles and MISTRAL_API_KEY:
        print(f"\n🤖 Analizando {min(len(new_articles),30)} artículos con Mistral...")
        for i, article in enumerate(new_articles[:30]):
            print(f"  [{i+1}] {article['title'][:70]}...")
            analysis = analyze_with_mistral(article)
            processed += 1

            if not analysis or analysis.get("level") == "irrelevant":
                print(f"    → Descartado")
                continue

            score = analysis.get("relevance_score", 0)
            if score < RELEVANCE_THRESHOLD:
                print(f"    → Score {score} < {RELEVANCE_THRESHOLD}, descartado")
                continue

            new_hits.append({
                "id":                 article["id"],
                "title":              article["title"],
                "link":               article["link"],
                "source":             article["source"],
                "raw_date":           article["date"],
                "fetched_at":         datetime.now(timezone.utc).isoformat(),
                "level":              analysis.get("level", "info"),
                "relevance_score":    analysis.get("relevance_score", 0),
                "risks":              analysis.get("risks", []),
                "norms_affected":     analysis.get("norms_affected", []),
                "summary":            analysis.get("summary", ""),
                "vulnerability":      analysis.get("vulnerability", ""),
                "financial_impact":   analysis.get("financial_impact", ""),
                "recommended_action": analysis.get("recommended_action", ""),
            })
            print(f"    ✓ [{analysis.get('level','?').upper()}] Score: {score}")
            time.sleep(1.2)

    elif not MISTRAL_API_KEY:
        # Sin Mistral: guardar artículos relevantes sin análisis IA
        print("\n⚠️  Sin API Mistral — guardando artículos sin análisis IA")
        for art in new_articles[:30]:
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
    all_hits = (new_hits + stored.get("hits", []))[:200]
    stats = {
        "total_hits":       len(all_hits),
        "critical_count":   sum(1 for h in all_hits if h.get("level") == "critical"),
        "warning_count":    sum(1 for h in all_hits if h.get("level") == "warning"),
        "info_count":       sum(1 for h in all_hits if h.get("level") == "info"),
        "new_this_run":     len(new_hits),
        "articles_checked": processed or len(new_articles),
    }
    save_hits({"hits": all_hits, "stats": stats, "last_updated": None})

    print(f"\n{'='*60}")
    print(f"✅ Resultado: {len(new_hits)} hits nuevos | Total: {len(all_hits)}")
    print(f"   Críticos: {stats['critical_count']} | Alertas: {stats['warning_count']} | Info: {stats['info_count']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
