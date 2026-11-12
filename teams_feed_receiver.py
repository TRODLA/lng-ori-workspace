import datetime as dt
import hmac
import json
import os
import re
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings

PORT = int(os.getenv("PORT", "8080"))
INGEST_PATH = "/api/teams-feed/ingest"
MAX_BODY = 256_000
DATABRICKS_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_TIMEOUT_SECONDS", "45"))
ALLOWED_CATEGORIES = {"Pricing & Commercial", "Demand & Supply", "Policy & Regulation", "Market Reports", "Other"}
ALLOWED_TYPES = {"Offer", "Bid", "Market indication", "Demand", "Supply", "Transaction", "Market observation", "Report", "Policy / Regulation", "Other"}
ALLOWED_INDEXES = {"HH", "JKM", "TTF", "Brent", "NBP", "Other"}
ALLOWED_CONTRACTS = {"SPA (DES)", "SPA (FOB)", "HOA", "MSPA", "Spot", "Other"}
ALLOWED_CONFIDENCE = {"High", "Medium", "Low"}

SYSTEM_PROMPT = """You are an LNG commercial market-intelligence extraction system.
Convert an informal Microsoft Teams message into structured LNG data.
The message may contain spelling mistakes, abbreviations, inconsistent capitalization, missing punctuation, LNG shorthand, uncertain language, or incomplete commercial terms.
Return ONLY one valid JSON object. Do not use markdown or explanatory text.

Never invent commercial information. If a field is not explicitly stated or strongly supported by the message, return null. Do not infer contract type, volume, start year, duration, region, or other terms merely from knowledge of a company.
Correct an obvious counterparty spelling variation only when the intended company is clear. Preserve the original spelling in counterparty_raw.
For a two-digit start year, convert to a four-digit year only when clearly used as a future commercial year, for example 28 means 2028.
Normalize volume to mtpa when the message explicitly provides a compatible LNG annual volume.
Preserve commercially meaningful pricing formulas, such as 120% HH + 4.50, 12.5% Brent + 0.50, or JKM - 0.20.

category must be exactly one of: Pricing & Commercial, Demand & Supply, Policy & Regulation, Market Reports, Other.
information_type must be exactly one of: Offer, Bid, Market indication, Demand, Supply, Transaction, Market observation, Report, Policy / Regulation, Other.
pricing_index, when present, must be exactly one of: HH, JKM, TTF, Brent, NBP, Other.
contract_type, when present, must be exactly one of: SPA (DES), SPA (FOB), HOA, MSPA, Spot, Other.
confidence must be exactly one of: High, Medium, Low.

Return exactly these properties:
category, information_type, counterparty_raw, counterparty, region, pricing_index, pricing_formula, contract_type, volume_mtpa, start_year, duration_years, confidence, summary.
Use null for unknown values. summary must be short, factual, and contain no information absent from the original message."""


def required(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def safe_text(value, limit):
    return str(value or "").strip()[:limit]


def safe_message_id(value):
    value = safe_text(value, 300)
    if not value or not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
        raise ValueError("A valid message_id is required.")
    return value


def storage_container():
    credential = ClientSecretCredential(
        tenant_id=required("AZURE_TENANT_ID"),
        client_id=required("AZURE_CLIENT_ID"),
        client_secret=required("AZURE_CLIENT_SECRET"),
    )
    account = required("AZURE_STORAGE_ACCOUNT_NAME")
    service = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=credential)
    return service.get_container_client(required("AZURE_STORAGE_CONTAINER_NAME"))


def nullable_text(value, limit=1000):
    if value is None:
        return None
    value = safe_text(value, limit)
    return value or None


def nullable_number(value, integer=False):
    if value in (None, ""):
        return None
    try:
        number = float(value)
        if integer:
            return int(number) if number.is_integer() else None
        return number
    except (TypeError, ValueError):
        return None


def extract_output_text(response):
    if isinstance(response.get("output_text"), str):
        return response["output_text"]
    for output in response.get("output", []):
        if not isinstance(output, dict):
            continue
        for content in output.get("content", []):
            if isinstance(content, dict) and content.get("type") == "output_text" and isinstance(content.get("text"), str):
                return content["text"]
    raise ValueError("Databricks response did not contain output_text.")


def parse_json_object(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
        text = re.sub(r"\s*```$", "", text)
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            raise ValueError("Databricks returned invalid JSON.")
        result = json.loads(match.group(0))
    if not isinstance(result, dict):
        raise ValueError("Databricks output was not a JSON object.")
    return result


def validate_extraction(raw):
    category = raw.get("category") if raw.get("category") in ALLOWED_CATEGORIES else "Other"
    information_type = raw.get("information_type") if raw.get("information_type") in ALLOWED_TYPES else "Other"
    pricing_index = raw.get("pricing_index") if raw.get("pricing_index") in ALLOWED_INDEXES else None
    contract_type = raw.get("contract_type") if raw.get("contract_type") in ALLOWED_CONTRACTS else None
    confidence = raw.get("confidence") if raw.get("confidence") in ALLOWED_CONFIDENCE else "Low"
    start_year = nullable_number(raw.get("start_year"), integer=True)
    duration_years = nullable_number(raw.get("duration_years"), integer=True)
    volume_mtpa = nullable_number(raw.get("volume_mtpa"))
    if start_year is not None and not 2020 <= start_year <= 2100:
        start_year = None
    if duration_years is not None and not 1 <= duration_years <= 50:
        duration_years = None
    if volume_mtpa is not None and not 0 < volume_mtpa <= 100:
        volume_mtpa = None
    return {
        "category": category,
        "information_type": information_type,
        "counterparty_raw": nullable_text(raw.get("counterparty_raw"), 300),
        "counterparty": nullable_text(raw.get("counterparty"), 300),
        "region": nullable_text(raw.get("region"), 300),
        "pricing_index": pricing_index,
        "pricing_formula": nullable_text(raw.get("pricing_formula"), 500),
        "contract_type": contract_type,
        "volume_mtpa": volume_mtpa,
        "start_year": start_year,
        "duration_years": duration_years,
        "confidence": confidence,
        "summary": nullable_text(raw.get("summary"), 1500),
    }


def call_databricks(message):
    endpoint = required("DATABRICKS_AI_GATEWAY_URL")
    token = required("DATABRICKS_TOKEN")
    model = required("DATABRICKS_AI_MODEL")
    body = {
        "model": model,
        "max_output_tokens": 1200,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_PROMPT}]},
            {"role": "user", "content": [{"type": "input_text", "text": "Analyse this LNG market-information message:\n\n" + message}]},
        ],
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=DATABRICKS_TIMEOUT_SECONDS) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"Databricks returned HTTP {exc.code}: {detail}") from exc
    extraction = validate_extraction(parse_json_object(extract_output_text(raw_response)))
    return extraction, raw_response.get("id"), raw_response.get("model") or model


def store_message(body):
    message_id = safe_message_id(body.get("message_id"))
    message = safe_text(body.get("message"), 20_000)
    if not message:
        raise ValueError("Message content is required.")
    received_at = dt.datetime.now(dt.timezone.utc).isoformat()
    envelope = {
        "schema_version": 2,
        "source": "Teams",
        "message_id": message_id,
        "chat": safe_text(body.get("chat"), 500) or "LNG ORI - Market Insights",
        "chat_id": safe_text(body.get("chat_id"), 500),
        "sender": safe_text(body.get("sender"), 500),
        "sender_id": safe_text(body.get("sender_id"), 500),
        "created_at": safe_text(body.get("created_at"), 100),
        "message": message,
        "message_link": safe_text(body.get("message_link"), 4000),
        "received_at": received_at,
        "processed": False,
        "ai_status": "pending",
        "ai_extraction": None,
    }
    try:
        extraction, response_id, model = call_databricks(message)
        envelope.update(
            ai_status="completed",
            ai_extraction=extraction,
            ai_model=model,
            ai_response_id=response_id,
            ai_processed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        )
    except Exception as exc:
        print("Databricks enrichment failed:", repr(exc), flush=True)
        envelope.update(ai_status="failed", ai_error=safe_text(exc, 2000))
    base = os.getenv("AZURE_STORAGE_BASE_DIRECTORY", "origination").strip("/")
    blob_path = f"{base}/teams_feed/{message_id}.json"
    raw = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
    client = storage_container().get_blob_client(blob_path)
    try:
        client.upload_blob(raw, overwrite=False, content_settings=ContentSettings(content_type="application/json; charset=utf-8"))
        created = True
    except ResourceExistsError:
        created = False
    return {
        "ok": True,
        "created": created,
        "message_id": message_id,
        "blob_path": blob_path,
        "ai_status": envelope["ai_status"],
        "ai_extraction": envelope["ai_extraction"],
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "ORI-TeamsFeed/2.0"

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", flush=True)

    def send_json(self, status, payload):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if urlparse(self.path).path == "/healthz":
            self.send_json(200, {"ok": True, "service": "teams-feed-receiver", "ai_enabled": bool(os.getenv("DATABRICKS_TOKEN"))})
        else:
            self.send_json(404, {"error": "Not found."})

    def do_POST(self):
        if urlparse(self.path).path != INGEST_PATH:
            self.send_json(404, {"error": "Not found."})
            return
        try:
            expected = required("TEAMS_FEED_INGEST_SECRET")
            supplied = self.headers.get("X-ORI-Teams-Feed-Key", "").strip()
            if not supplied or not hmac.compare_digest(expected, supplied):
                self.send_json(403, {"error": "Invalid Teams feed key."})
                return
            if "application/json" not in self.headers.get("Content-Type", ""):
                raise ValueError("Expected application/json.")
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                raise ValueError("Request body is empty or too large.")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("Expected a JSON object.")
            result = store_message(body)
            self.send_json(201 if result["created"] else 200, result)
        except (ValueError, json.JSONDecodeError) as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception as exc:
            print("Teams feed ingestion failed:", repr(exc), flush=True)
            self.send_json(503, {"error": "The Teams message could not be stored."})


if __name__ == "__main__":
    print(f"Starting Teams feed receiver with Databricks enrichment on port {PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

