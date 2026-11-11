import datetime as dt
import hmac
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError

PORT = int(os.getenv("PORT", "8080"))
INGEST_PATH = "/api/teams-feed/ingest"
MAX_BODY = 256_000


def required(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def storage_container():
    credential = ClientSecretCredential(
        tenant_id=required("AZURE_TENANT_ID"),
        client_id=required("AZURE_CLIENT_ID"),
        client_secret=required("AZURE_CLIENT_SECRET"),
    )
    account = required("AZURE_STORAGE_ACCOUNT_NAME")
    service = BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=credential,
    )
    return service.get_container_client(required("AZURE_STORAGE_CONTAINER_NAME"))


def safe_text(value, limit):
    return str(value or "").strip()[:limit]


def safe_message_id(value):
    value = safe_text(value, 300)
    if not value or not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
        raise ValueError("A valid message_id is required.")
    return value


def store_message(body):
    message_id = safe_message_id(body.get("message_id"))
    message = safe_text(body.get("message"), 20_000)
    if not message:
        raise ValueError("Message content is required.")

    envelope = {
        "schema_version": 1,
        "source": "Teams",
        "message_id": message_id,
        "chat": safe_text(body.get("chat"), 500) or "LNG ORI - Market Insights",
        "chat_id": safe_text(body.get("chat_id"), 500),
        "sender": safe_text(body.get("sender"), 500),
        "sender_id": safe_text(body.get("sender_id"), 500),
        "created_at": safe_text(body.get("created_at"), 100),
        "message": message,
        "message_link": safe_text(body.get("message_link"), 4000),
        "received_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "processed": False,
    }

    base = os.getenv("AZURE_STORAGE_BASE_DIRECTORY", "origination").strip("/")
    blob_path = f"{base}/teams_feed/{message_id}.json"
    raw = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
    client = storage_container().get_blob_client(blob_path)
    try:
        client.upload_blob(
            raw,
            overwrite=False,
            content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
        )
        created = True
    except ResourceExistsError:
        created = False
    return {"ok": True, "created": created, "message_id": message_id, "blob_path": blob_path}


class Handler(BaseHTTPRequestHandler):
    server_version = "ORI-TeamsFeed/1.0"

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
            self.send_json(200, {"ok": True, "service": "teams-feed-receiver"})
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
    print(f"Starting Teams feed receiver on port {PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
