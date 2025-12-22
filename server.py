from http.server import HTTPServer, SimpleHTTPRequestHandler
import os

port = int(os.environ.get('PORT', 10000))

class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

print(f"Serving on port {port}")
HTTPServer(('', port), Handler).serve_forever()
