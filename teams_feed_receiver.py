import datetime as dt
import hmac
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings

PORT = int(os.getenv("PORT", "8080"))
INGEST_PATH = "/api/teams-feed/ingest"
MAX_BODY = 1_000_000
ALLOWED_CATEGORIES = {"Pricing & Commercial", "Demand & Supply", "Policy & Regulation", "Market Reports", "Other"}
ALLOWED_TYPES = {"Offer", "Bid", "Market indication", "Demand", "Supply", "Transaction", "Market observation", "Report", "Policy / Regulation", "Other"}
ALLOWED_INDEXES = {"HH", "JKM", "TTF", "Brent", "NBP", "Other"}
ALLOWED_CONTRACTS = {"SPA (DES)", "SPA (FOB)", "HOA", "MSPA", "Spot", "Other"}
ALLOWED_CONFIDENCE = {"High", "Medium", "Low"}

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
















def _feed_ai_unwrap(value):
    """Accept direct JSON, serialized predictionOutput, or legacy text wrappers."""
    for _ in range(8):
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.startswith('```'):
                text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text, flags=re.I)
            value = json.loads(text)
            continue
        if not isinstance(value, dict):
            raise ValueError('AI extraction must be a JSON object.')
        for key in ('structuredOutput', 'predictionOutput'):
            if value.get(key) is not None:
                value = value[key]
                break
        else:
            if 'category' in value or 'pricing_formula' in value or 'counterparty' in value:
                return value
            if value.get('text'):
                value = value['text']
                continue
            raise ValueError('No structured LNG fields found in AI output.')
    raise ValueError('AI output contains too many nested wrappers.')


def _feed_ai_validate(raw):
    import math
    if not isinstance(raw, dict):
        raise ValueError('Expected structured LNG fields.')
    def text(key, limit=500):
        value = raw.get(key)
        if value is None:
            return None
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise ValueError('Invalid text field: ' + key)
        value = str(value).strip()
        return None if value.lower() in ('', 'null', 'none', 'not identified', 'not set') else value[:limit]
    def enum(key, allowed, default=None):
        value = text(key)
        if value is None:
            return default
        for candidate in allowed:
            if candidate.lower() == value.lower():
                return candidate
        raise ValueError('Unsupported ' + key + ': ' + value)
    def number(key, integer=False):
        value = raw.get(key)
        if value is None or (isinstance(value, str) and value.strip().lower() in ('', 'null', 'none')):
            return None
        if isinstance(value, bool):
            raise ValueError('Invalid numeric field: ' + key)
        value = float(value)
        if not math.isfinite(value) or value < 0:
            raise ValueError('Invalid numeric field: ' + key)
        if integer and not value.is_integer():
            raise ValueError('Expected a whole number: ' + key)
        return int(value) if integer else value
    result = {
        'category': enum('category', ['Pricing & Commercial','Demand & Supply','Policy & Regulation','Market Reports','Other'], 'Other'),
        'information_type': enum('information_type', ['Offer','Bid','Market indication','Demand','Supply','Transaction','Market observation','Report','Policy / Regulation','Other'], 'Other'),
        'counterparty_raw': text('counterparty_raw', 300),
        'counterparty': text('counterparty', 300),
        'region': text('region', 300),
        'pricing_index': enum('pricing_index', ['HH','JKM','TTF','Brent','NBP','THE','Hybrid','Other']),
        'pricing_formula': text('pricing_formula', 1000),
        'contract_type': enum('contract_type', ['SPA (DES)','SPA (FOB)','HOA','MSPA','Spot','Other']),
        'volume_mtpa': number('volume_mtpa'),
        'start_year': number('start_year', True),
        'duration_years': number('duration_years', True),
        'confidence': enum('confidence', ['High','Medium','Low'], 'Low'),
        'summary': text('summary', 3000),
    }
    if result['start_year'] is not None and not 1900 <= result['start_year'] <= 2200:
        raise ValueError('Start year is outside the supported range.')
    if result['duration_years'] is not None and not 1 <= result['duration_years'] <= 100:
        raise ValueError('Duration must be between 1 and 100 years.')
    return result


def _feed_source_link(value, chat_id, message_id):
    from urllib.parse import urlparse, quote
    value = str(value or '').strip()
    parsed = urlparse(value)
    if parsed.scheme == 'https' and parsed.hostname in ('teams.microsoft.com', 'teams.cloud.microsoft'):
        return value
    if chat_id and message_id:
        return 'https://teams.microsoft.com/l/message/' + quote(str(chat_id), safe='') + '/' + quote(str(message_id), safe='') + '?context=%7B%22contextType%22%3A%22chat%22%7D'
    return ''


def store_message(body):
    message_id = safe_message_id(body.get('message_id'))
    original = body.get('message')
    if not isinstance(original, str) or not original.strip():
        raise ValueError('Message content is required.')
    if len(original) > 100_000:
        raise ValueError('Message exceeds the 100000 character limit.')
    chat_id = safe_text(body.get('chat_id'), 500)
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    envelope = {
        'schema_version': 3, 'source': 'Teams', 'message_id': message_id,
        'chat': safe_text(body.get('chat'), 500) or 'LNG ORI - Market Insights',
        'chat_id': chat_id, 'sender': safe_text(body.get('sender'), 500),
        'sender_id': safe_text(body.get('sender_id'), 500),
        'created_at': safe_text(body.get('created_at'), 100),
        'message': original, 'original_message': original,
        'message_link': _feed_source_link(body.get('message_link'), chat_id, message_id),
        'received_at': timestamp, 'processed': False,
        'ai_status': 'not_provided', 'ai_extraction': None,
    }
    supplied = body.get('ai_extraction')
    if supplied is not None and supplied != '':
        try:
            unwrapped = _feed_ai_unwrap(supplied)
            if unwrapped is not None:
                extraction = _feed_ai_validate(unwrapped)
                envelope.update(ai_status='completed', ai_extraction=extraction,
                                ai_original_extraction=extraction.copy(),
                                ai_provider='Power Automate AI Builder', ai_processed_at=timestamp)
        except (ValueError, TypeError, OverflowError) as exc:
            envelope.update(ai_status='invalid', ai_error=str(exc)[:500])
    # No Databricks request is made. Raw messages remain ingestible without AI.
    base = os.getenv('AZURE_STORAGE_BASE_DIRECTORY', 'origination').strip('/')
    blob_path = '/'.join(x for x in (base, 'teams_feed', message_id + '.json') if x)
    client = storage_container().get_blob_client(blob_path)
    settings = ContentSettings(content_type='application/json; charset=utf-8')
    created, enriched_existing = False, False
    try:
        client.upload_blob(json.dumps(envelope, ensure_ascii=False, allow_nan=False).encode('utf-8'),
                           overwrite=False, content_settings=settings)
        created = True
    except ResourceExistsError:
        # Upgrade a pending legacy envelope, but never overwrite a processed or
        # manually reviewed envelope. ETags protect concurrent updates.
        for attempt in range(3):
            download = client.download_blob()
            existing = json.loads(download.readall().decode('utf-8'))
            if (not existing.get('processed') and not existing.get('edited_at')
                    and not existing.get('ai_reviewed_at')
                    and existing.get('ai_status') != 'completed'
                    and envelope['ai_status'] == 'completed'):
                for key in ('ai_status','ai_extraction','ai_original_extraction','ai_provider','ai_processed_at'):
                    existing[key] = envelope[key]
                existing['schema_version'] = 3
                existing.setdefault('original_message', existing.get('message', original))
                existing['message_link'] = _feed_source_link(existing.get('message_link'), existing.get('chat_id'), message_id)
                existing.pop('ai_error', None)
                try:
                    client.upload_blob(json.dumps(existing, ensure_ascii=False, allow_nan=False).encode('utf-8'),
                                       overwrite=True, content_settings=settings,
                                       etag=download.properties.etag, match_condition=MatchConditions.IfNotModified)
                    enriched_existing = True
                except ResourceModifiedError:
                    if attempt < 2:
                        continue
                    raise
            envelope = existing
            break
    return {'ok': True, 'created': created, 'enriched_existing': enriched_existing,
            'message_id': message_id, 'blob_path': blob_path,
            'ai_status': envelope.get('ai_status', 'not_provided'),
            'ai_extraction': envelope.get('ai_extraction'),
            'ai_error': envelope.get('ai_error')}


class Handler(BaseHTTPRequestHandler):
    server_version = "ORI-TeamsFeed/3.0"

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
            self.send_json(200, {"ok": True, "service": "teams-feed-receiver", "ai_source": "Power Automate AI Builder", "accepts_ai_extraction": True})
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



# Outlook ingestion shares the existing service/secret, but never Teams IDs or paths.
def _outlook57_plain_html(value):
    from html.parser import HTMLParser
    class Reader(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts = []
            self.skip = 0
        def handle_starttag(self, tag, attrs):
            if tag in ('script', 'style', 'head'):
                self.skip += 1
            if not self.skip and tag in ('p','div','br','tr','li','h1','h2','h3','blockquote'):
                self.parts.append('\n')
        def handle_endtag(self, tag):
            if tag in ('script', 'style', 'head') and self.skip:
                self.skip -= 1
            if not self.skip and tag in ('p','div','tr','li','h1','h2','h3','blockquote'):
                self.parts.append('\n')
        def handle_data(self, data):
            if not self.skip:
                self.parts.append(data)
    parser = Reader()
    parser.feed(value)
    return re.sub(r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n', ''.join(parser.parts)).strip()


def _outlook57_link(value):
    parsed = urlparse(str(value or '').strip())
    if (parsed.scheme == 'https' and parsed.hostname in
            ('outlook.office.com','outlook.office365.com','outlook.cloud.microsoft')
            and not parsed.username and not parsed.password):
        return str(value).strip()
    return ''


def store_outlook_message(body):
    import hashlib
    mailbox = str(body.get('mailbox') or '').strip().lower()
    if not re.fullmatch(r'[^\s@/]+@[^\s@/]+\.[^\s@/]+', mailbox):
        raise ValueError('A valid shared mailbox email address is required.')
    message_id = str(body.get('message_id') or body.get('id') or '').strip()
    internet_id = str(body.get('internet_message_id') or body.get('internetMessageId') or '').strip()
    if not message_id or len(message_id) > 4096 or len(internet_id) > 4096:
        raise ValueError('A valid email message_id is required (maximum 4096 characters).')
    subject = str(body.get('subject') or '').strip()
    html = body.get('body_html') or ''
    text = body.get('body_text') or ''
    if not html and not text and isinstance(body.get('body'), str):
        if body.get('isHtml') is False:
            text = body['body']
        else:
            html = body['body']
    if not isinstance(html, str) or not isinstance(text, str):
        raise ValueError('Email body must be text.')
    if len(html) > 600000 or len(text) > 300000 or len(subject) > 2000:
        raise ValueError('Email exceeds the supported size; attachments are not supported.')
    plain = text.strip() if text.strip() else _outlook57_plain_html(html)
    if not plain and not subject:
        raise ValueError('An email subject or body is required.')
    # Graph/connector IDs contain unsafe filename characters. A mailbox-scoped
    # SHA-256 key is stable for replay and never embeds the raw email ID in a path.
    key = hashlib.sha256((mailbox + '\n' + (internet_id or message_id)).encode('utf-8')).hexdigest()
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    envelope = {
        'schema_version': 4, 'source': 'Outlook', 'feed_id': key,
        'message_id': message_id, 'internet_message_id': internet_id, 'mailbox': mailbox,
        'subject': subject, 'sender': safe_text(body.get('sender') or body.get('from'), 500),
        'sender_email': safe_text(body.get('sender_email') or body.get('from'), 500),
        'created_at': safe_text(body.get('received_at') or body.get('created_at') or body.get('receivedDateTime'), 100),
        'received_at': timestamp, 'message': plain, 'original_message': plain,
        'original_html': html, 'message_link': _outlook57_link(body.get('message_link')),
        'processed': False, 'ai_status': 'not_provided', 'ai_extraction': None,
        'attachments_processed': False,
    }
    if body.get('ai_extraction') not in (None, ''):
        try:
            raw = _feed_ai_unwrap(body['ai_extraction'])
            if raw is not None:
                ai = _feed_ai_validate(raw)
                envelope.update(ai_status='completed', ai_extraction=ai,
                    ai_original_extraction=ai.copy(), ai_provider='Power Automate AI Builder',
                    ai_processed_at=timestamp)
        except (ValueError, TypeError, OverflowError) as exc:
            envelope.update(ai_status='invalid', ai_error=str(exc)[:500])
    base = os.getenv('AZURE_STORAGE_BASE_DIRECTORY', 'origination').strip('/')
    path = '/'.join(x for x in (base, 'outlook_feed', key + '.json') if x)
    client = storage_container().get_blob_client(path)
    settings = ContentSettings(content_type='application/json; charset=utf-8')
    created, enriched = False, False
    def encoded(item):
        return json.dumps(item, ensure_ascii=False, allow_nan=False).encode('utf-8')
    try:
        client.upload_blob(encoded(envelope), overwrite=False, content_settings=settings)
        created = True
    except ResourceExistsError:
        for attempt in range(3):
            download = client.download_blob()
            existing = json.loads(download.readall().decode('utf-8'))
            if (not existing.get('processed') and not existing.get('edited_at')
                    and not existing.get('ai_reviewed_at')
                    and existing.get('ai_status') != 'completed'
                    and envelope['ai_status'] == 'completed'):
                for name in ('ai_status','ai_extraction','ai_original_extraction','ai_provider','ai_processed_at'):
                    existing[name] = envelope[name]
                existing.pop('ai_error', None)
                try:
                    client.upload_blob(encoded(existing), overwrite=True, content_settings=settings,
                        etag=download.properties.etag, match_condition=MatchConditions.IfNotModified)
                    enriched = True
                except ResourceModifiedError:
                    if attempt < 2:
                        continue
                    raise
            envelope = existing
            break
    return {'ok': True, 'source': 'Outlook', 'created': created, 'enriched_existing': enriched,
        'feed_id': key, 'message_id': message_id, 'blob_path': path,
        'ai_status': envelope.get('ai_status', 'not_provided'),
        'ai_extraction': envelope.get('ai_extraction'), 'ai_error': envelope.get('ai_error')}


_TEAMS57_POST = Handler.do_POST
_TEAMS57_GET = Handler.do_GET

def _outlook57_get(handler):
    if urlparse(handler.path).path == '/healthz':
        handler.send_json(200, {'ok': True, 'service': 'ori-market-feed-receiver',
            'sources': ['Teams','Outlook'], 'accepts_ai_extraction': True})
        return
    return _TEAMS57_GET(handler)


def _outlook57_post(handler):
    if urlparse(handler.path).path != '/api/outlook-feed/ingest':
        return _TEAMS57_POST(handler)
    try:
        # Reuse the existing authenticated ingestion connection. No new Radix
        # secret is needed. Do not change the Teams flow or its header.
        expected = required('TEAMS_FEED_INGEST_SECRET')
        supplied = handler.headers.get('X-ORI-Teams-Feed-Key', '').strip()
        if not supplied or not hmac.compare_digest(expected, supplied):
            handler.send_json(403, {'error': 'Invalid feed key.'})
            return
        if handler.headers.get('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
            raise ValueError('Expected application/json.')
        length = int(handler.headers.get('Content-Length', '0'))
        if length <= 0 or length > MAX_BODY:
            raise ValueError('Request body is empty or too large.')
        body = json.loads(handler.rfile.read(length))
        if not isinstance(body, dict):
            raise ValueError('Expected a JSON object.')
        result = store_outlook_message(body)
        handler.send_json(201 if result['created'] else 200, result)
    except (ValueError, TypeError) as exc:
        handler.send_json(400, {'error': str(exc)})
    except Exception as exc:
        print('Outlook feed ingestion failed:', type(exc).__name__, flush=True)
        handler.send_json(503, {'error': 'The Outlook message could not be stored.'})

Handler.do_GET = _outlook57_get
Handler.do_POST = _outlook57_post
Handler.server_version = 'ORI-MarketFeed/4.0'

if __name__ == "__main__":
    print(f"Starting Teams feed receiver with AI Builder payload support on port {PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

