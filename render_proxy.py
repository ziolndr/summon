import json
import mimetypes
import os
import posixpath
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

DEFAULT_BACKEND = "https://89kzehob57fw.shares.zrok.io"
ROOT = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", "10000"))
BACKEND = os.environ.get(
    "SUMMON_BACKEND_URL",
    DEFAULT_BACKEND,
).rstrip("/")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "SUMMONRenderProxy/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        print(
            f"[http] {self.address_string()} · {fmt % args}",
            flush=True,
        )

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, HEAD, OPTIONS",
        )
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_HEAD(self) -> None:
        self._dispatch(head_only=True)

    def do_GET(self) -> None:
        self._dispatch(head_only=False)

    def do_POST(self) -> None:
        self._proxy(head_only=False)

    def _dispatch(self, head_only: bool) -> None:
        path = urlsplit(self.path).path

        if path == "/health":
            payload = json.dumps(
                {
                    "ok": True,
                    "mode": "stateless-proxy",
                    "backend": BACKEND,
                }
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self._cors()
            self.end_headers()

            if not head_only:
                self.wfile.write(payload)
            return

        if path == "/field" or path.startswith("/field/"):
            self._proxy(head_only=head_only)
            return

        self._serve_static(path, head_only=head_only)

    def _proxy(self, head_only: bool) -> None:
        target = BACKEND + self.path
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length else None

        blocked_request_headers = {
            "host",
            "user-agent",
            "accept-encoding",
            "origin",
            "referer",
        }

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP
            and key.lower() not in blocked_request_headers
        }

        # zrok can return an HTML browser interstitial when Chrome's
        # request headers are forwarded upstream. Render must identify
        # itself as a server-side proxy and explicitly skip that page.
        headers["Accept"] = "application/json"
        headers["User-Agent"] = "SUMMON-render-proxy/1.0"
        headers["skip_zrok_interstitial"] = "1"

        request = Request(
            target,
            data=body,
            headers=headers,
            method=self.command,
        )

        try:
            with urlopen(request, timeout=180) as response:
                payload = response.read()
                self.send_response(response.status)

                for key, value in response.headers.items():
                    lower = key.lower()
                    if lower in HOP_BY_HOP or lower == "content-length":
                        continue
                    self.send_header(key, value)

                self.send_header("Content-Length", str(len(payload)))
                self._cors()
                self.end_headers()

                if not head_only:
                    self.wfile.write(payload)

        except HTTPError as error:
            payload = error.read()
            self.send_response(error.code)
            self.send_header(
                "Content-Type",
                error.headers.get(
                    "Content-Type",
                    "application/json",
                ),
            )
            self.send_header("Content-Length", str(len(payload)))
            self._cors()
            self.end_headers()

            if not head_only:
                self.wfile.write(payload)

        except (URLError, TimeoutError) as error:
            payload = json.dumps(
                {
                    "error": "SUMMON backend unavailable",
                    "backend": BACKEND,
                    "detail": str(error),
                }
            ).encode("utf-8")

            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self._cors()
            self.end_headers()

            if not head_only:
                self.wfile.write(payload)

    def _serve_static(
        self,
        request_path: str,
        head_only: bool,
    ) -> None:
        decoded = posixpath.normpath(
            request_path.split("?", 1)[0]
        ).lstrip("/")

        relative = decoded or "index.html"
        candidate = (ROOT / relative).resolve()

        try:
            candidate.relative_to(ROOT)
        except ValueError:
            self.send_error(403)
            return

        if candidate.is_dir():
            candidate = candidate / "index.html"

        if not candidate.is_file():
            candidate = ROOT / "index.html"

        payload = candidate.read_bytes()
        content_type = (
            mimetypes.guess_type(candidate.name)[0]
            or "application/octet-stream"
        )

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header(
            "Cache-Control",
            "no-cache"
            if candidate.name == "index.html"
            else "public, max-age=3600",
        )
        self._cors()
        self.end_headers()

        if not head_only:
            self.wfile.write(payload)


if __name__ == "__main__":
    print(
        f"SUMMON RENDER PROXY · 0.0.0.0:{PORT} → {BACKEND}",
        flush=True,
    )
    ThreadingHTTPServer(
        ("0.0.0.0", PORT),
        Handler,
    ).serve_forever()
