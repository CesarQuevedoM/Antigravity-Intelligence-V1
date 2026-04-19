# ⚡ Antigravity Intelligence V1

**Infrastructure Layer — B2B Market Intelligence & Shopify Scanner**

Antigravity Intelligence is a modular Python-based data pipeline designed to analyze B2B lead lists, specifically targeting e-commerce stores in the US market. It acts as an automated triage system, scraping live data to qualify leads before initiating cold outreach.

## 🧠 Core Features

* **Shopify Sitemap Hacker:** Automatically connects to `sitemap_products.1.xml` to bypass front-end blockers.
* **Dual XML Parsing Engine:** Utilizes `xml.etree` for high-speed parsing, with an automatic fallback to `BeautifulSoup` (`lxml-xml`) for malformed Shopify templates.
* **Robust Data Ingestion:** Uses Pandas with custom separator detection and bad-line skipping to handle corrupted CSV lead lists seamlessly.
* **Anti-Bot Evasion:** Implements rotating User-Agents to prevent IP fingerprinting.

## 📊 The Tier Scoring Logic

The system automatically scores each store to prioritize high-value targets ("Big Fish"):

* **Tier A (Hot Leads):** > 30 products AND catalog updated within the last 30 days.
* **Tier B (Nurture):** 1-29 products OR catalog updated within the last 6 months.
* **Tier C (Discard):** 0 products, 404 errors, or non-Shopify architectures.

## 🛠️ Tech Stack
* Python 3.12+
* Pandas (Data manipulation)
* Requests (HTTP protocol)
* BeautifulSoup4 & lxml (XML Parsing)

*Developed for the B2B US Market.*
