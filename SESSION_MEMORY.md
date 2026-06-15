# Session Memory — Akhil Bhardwaj | Career & Interview Prep
Last updated: 2026-06-14

---

## Who Akhil Is
- **Name:** Akhil Bhardwaj
- **Email:** akh.bhardwaj@gmail.com
- **Location:** Amsterdam (EU passport holder)
- **Profile:** Cloud & Data Architect with deep Azure expertise — Databricks, AKS, APIM, Event Hub, Delta Lake, MLflow, Terraform, microservices
- **Career goal:** Convert a small number of high-quality opportunities rather than mass-applying. Deliberately slowing down applications to focus and convert.

---

## Emotional & Strategic Context
- Akhil received a **rejection from Databricks** (Field Engineering role) in this session — was heartbroken. The evaluation in the Design & Architecture stage was not supportive. He has asked for a feedback call with Michael (Databricks recruiter) on **Tuesday at 5:30pm**.
- His response to the rejection was mature: slow down, apply to fewer roles, prepare more deeply. MTU is the primary focus right now.
- He believes detailed demos and elaborate presentations will increase conversion chances — and we agreed this is the right strategy for a hands-on architect role.
- He is doing all of this preparation work seriously and methodically — building real code, real Azure infrastructure, real decks.

---

## Active Opportunity: MTU Aero Engines
- **Role:** Senior Cloud & Software Architect
- **Recruiter:** Emre Kiroglu, Nicholls Advisory (executive search)
- **Location:** Amsterdam, Hybrid
- **Package:** €7,550/month + 24% personal bonus + 8% Dutch holiday allowance
- **Eligibility:** EU passport required ✓
- **JD tone:** "This is not theory. This is production. This is high-stakes." (read this twice)
- **Key requirements:** Hands-on coding (Python/Java/C#), Azure (AKS/APIM/Databricks/DevOps), microservices, event-driven systems, Kubernetes/Docker, CI/CD, IaC, Databricks lakehouse, AI/ML in production, FinOps mindset
- **What they explicitly don't want:** Governance-only people, those who haven't coded in years, people who need detailed instructions

### MTU Company Context
- €7.2bn revenue, ~12,000 employees, DAX-listed, Munich HQ
- CEO: Lars Wagner
- Makes: GTF engines (PW1100G), EJ200 (Eurofighter), TP400 (A400M), V2500, CF6
- 14 MRO sites globally: Hannover, Berlin, Zhuhai, Fort Worth, Serbia, Canada, Brazil, Australia + others
- Key systems: ETMWeb (maintenance tracking), ACARS/ACARSplus (flight data), FADEC (engine control)
- Compliance: EASA Part 21/145/M/66/147, FAA/BASA, AS9100D, DO-178C (FADEC DAL-A), ITAR/EAR, GDPR/BDSG/NIS2, ISO 27001, CSRD/ESRS, CORSIA, EU ETS

---

## Decks Built for MTU (all in Tech Mahindra folder)
| File | Slides | Content |
|---|---|---|
| end_to_end_demo_architecture.pptx | 18 | Full Azure demo — all 9 components with production code |
| mtu_compliance_audit_requirements.pptx | 14 | EASA/FAA/ITAR/GDPR/NIS2/CSRD compliance detail |
| mtu_compliance_architecture.pptx | 13 | Architecture satisfying each compliance domain |
| mtu_finops.pptx | 13 | FinOps framework, unit economics, 4 mindset slides |

### Deck colour palette (MTU brand): NAVY=#0A1628, ORANGE=#E8610A, TEAL=#0D9488
### All decks built with PptxGenJS (Node.js) — scripts in outputs folder

---

## Demo Architecture (9 components)
The core demo Akhil is building for MTU covers:
1. **Terraform** — IaC for all Azure resources
2. **Event Hub** — real-time engine telemetry ingestion
3. **Stream Analytics** — real-time anomaly flagging
4. **Databricks Bronze** — raw landing (Delta Lake, WORM)
5. **Databricks Silver** — validated, cleansed data
6. **Databricks Gold** — business-ready aggregations + MLflow model
7. **FastAPI** — microservice serving predictions
8. **AKS** — Kubernetes deployment of FastAPI
9. **APIM** — API gateway
10. **GitHub Actions** — CI/CD pipeline

---

## Current Active Work: Live Streaming Demo (OpenSky → Event Hub → Databricks)

### What we're building
Real aircraft from OpenSky Network → Azure Event Hub → Databricks Structured Streaming → Delta Bronze table (live rows appearing in notebook)

### Status: PAUSED at cluster creation
- New Databricks workspace created: **akhils2nddatabricks** (Hybrid, Premium, West Europe)
- User is inside the new workspace, about to create a cluster
- **Next step when returning:** Create a cluster

### Why first Databricks workspace failed
- First workspace (akhilsdatabricksworkspace) was created in **Serverless mode** — no classic clusters available
- All cluster URLs redirected to SQL warehouses
- Created new workspace with **Hybrid** type — this has full cluster support

### Why OpenSky doesn't work from Databricks
- OpenSky blocks Azure datacenter IP ranges
- Solution: run OpenSky poller LOCALLY in VS Code, send to Event Hub, Databricks reads from Event Hub
- Internet works fine from Databricks (httpbin.org returns 200), just OpenSky is blocked

### Next Steps When Returning
**Step 1 — Create cluster in akhils2nddatabricks**
- Compute → All-purpose clusters → Create compute
- Name: opensky-cluster, Single node, Runtime 13.3 LTS, Standard_DS3_v2

**Step 2 — Install Maven library**
- Cluster → Libraries → Install New → Maven
- `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
- Restart cluster after install

**Step 3 — Create notebook**
- Workspace → Create → Notebook → Python → attach to opensky-cluster
- Paste from `databricks_eventhub_stream.py`

**Step 4 — Run local script**
```bash
pip install azure-eventhub requests
python opensky_to_eventhub.py
```

**Step 5 — Run notebook cells 1→2→3→4, then Cell 5 for live display**

---

## Azure Resources Created
| Resource | Name | Details |
|---|---|---|
| Resource Group | AkhilsJuneResourceGroup | West Europe |
| Event Hub Namespace | akhilseventhub | Basic, 1 TU, West Europe |
| Event Hub | opensky-feed | 1 partition, 24hr retention |
| Databricks (OLD — don't use) | akhilsdatabricksworkspace | Serverless, no clusters |
| Databricks (NEW — use this) | akhils2nddatabricks | Hybrid, Premium, has clusters |

### Event Hub Connection String
```
Endpoint=sb://akhilseventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=kfgM4MOJ1weKeNpEFut430/++JwpzcM9R+AEhIdDAQE=
```
⚠️ REGENERATE THIS KEY after the MTU interview — it was shared in chat.

---

## Code Files (all in Tech Mahindra folder)
| File | Purpose | Run where |
|---|---|---|
| opensky_live.py | Live aircraft table in terminal | VS Code only |
| opensky_to_csv.py | Saves timestamped CSV every 30s | VS Code, saves to CodeExp2\Data |
| opensky_to_eventhub.py | Polls OpenSky, sends to Event Hub | VS Code (local) |
| databricks_eventhub_stream.py | Reads Event Hub → Delta Bronze | Databricks notebook |

---

## Other Pending Work (deferred)
- Debug Demo 5 (multi-agent pipeline) and Demo 6 (Streamlit app)
- Build LangChain, LangGraph, LangSmith, LangFlow, MCP demos
- Databricks feedback call with Michael — Tuesday 5:30pm (collect specific feedback on Field Engineering architecture evaluation format)

---

## Technical Patterns Established
- **Frogcast API:** Akhil has working Python code calling solar forecast API (lat/lng/time_step/horizon). Azure Functions is the right ingestion tool (not ADF, not Logic Apps for transformation)
- **Template literal escaping in PptxGenJS:** All `${...}` in Terraform/K8s/GitHub Actions code strings must be escaped as `\${...}` to avoid JS syntax errors
- **Databricks cost:** Databricks dominates Azure spend at ~32% — justified, not to be cut blindly
- **MTU data classes:** ITAR-CONTROLLED, CERT-BOUNDARY, GDPR-PERSONAL, COMMERCIAL-UNCLASSIFIED
- **Three hard boundaries:** ITAR tenant separation, DO-178C one-way data flow, GDPR EU-only residency
- **LLP immutability:** WORM storage (Azure immutability policy), Delta Lake append-only, Unity Catalog DELETE denial
