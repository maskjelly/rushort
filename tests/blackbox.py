"""Independent HTTP fixtures: fail-closed benchmark checks and process durability."""
import contextlib
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / 'target/release'
KEY = 'local-test-key-' + 'x' * 32

@contextlib.contextmanager
def server(*args):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, RUSHORT_API_KEY=KEY)
    process = subprocess.Popen([str(BIN/'shortener'), '--bind', f'127.0.0.1:{port}', *args], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(200):
            if process.poll() is not None:
                raise RuntimeError('server failed to start')
            try:
                with socket.create_connection(('127.0.0.1', port), .1):
                    break
            except OSError:
                time.sleep(.01)
        else:
            raise RuntimeError('server did not become ready')
        yield port, process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()


def load(port, *args):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)/'result.json'
        p = subprocess.run([str(BIN/'loadgen'), '--target', f'http://127.0.0.1:{port}', '--seed', '1', '--connections', '1', '--duration', '.2', '--json', str(path), *args], env=dict(os.environ, RUSHORT_API_KEY=KEY), capture_output=True, text=True, timeout=15)
        return p, json.loads(path.read_text()) if path.exists() else None


class Fixture(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def setup(self):
        super().setup()
        with self.server.lock:
            self.server.connections += 1
            self.connection_number = self.server.connections
    def log_message(self, *_): pass
    def respond(self, status, body=b'', **headers):
        self.send_response(status)
        self.send_header('Content-Length', str(len(body)))
        for k, v in headers.items(): self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.server.lock:
            index = self.server.posts
            self.server.posts += 1
        url = body['url']
        if index and self.server.behavior == 'wrong_url': url = 'https://wrong.invalid/'
        code = '0' if self.server.behavior == 'duplicate' else str(index)
        self.server.urls[code] = url
        self.respond(201, json.dumps(dict(code=code, long_url=url)).encode())
    def do_GET(self):
        measured = self.connection_number == 2
        if measured:
            self.server.times.append(time.monotonic())
            if self.server.behavior == '500':
                return self.respond(500)
            if self.server.behavior == 'slow': time.sleep(.02)
        code = self.path[1:]
        if code in self.server.urls: self.respond(302, Location=self.server.urls[code])
        else: self.respond(404)


@contextlib.contextmanager
def fixture(behavior):
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Fixture)
    srv.daemon_threads = True
    srv.behavior = behavior; srv.posts = 0; srv.connections = 0
    srv.urls = {}; srv.times = []; srv.lock = threading.Lock()
    thread = threading.Thread(target=srv.serve_forever, daemon=True); thread.start()
    try: yield srv
    finally: srv.shutdown(); srv.server_close(); thread.join()


class BenchmarkTests(unittest.TestCase):
    def test_every_500_fails(self):
        with fixture('500') as srv:
            p, r = load(srv.server_port, '--mode', 'saturate')
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertGreater(r['unexpected_status'], 0)
            self.assertEqual(r['ok'], 0)
            self.assertFalse(r['pass'])

    def test_wrong_submitted_url_fails(self):
        with fixture('wrong_url') as srv:
            p, r = load(srv.server_port, '--mode', 'saturate', '--write-ratio', '1')
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertGreater(r['mismatches'], 0)

    def test_duplicate_codes_fail(self):
        with fixture('duplicate') as srv:
            p, r = load(srv.server_port, '--mode', 'saturate', '--write-ratio', '1')
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertIn('duplicate', r['verification_error'])

    def test_saturation_has_no_unmeasured_half_second(self):
        with fixture('good') as srv:
            p, r = load(srv.server_port, '--mode', 'saturate')
            self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
            self.assertGreater(len(srv.times), 1)
            self.assertLess(srv.times[-1] - srv.times[0], .4)
            self.assertGreaterEqual(r['elapsed_s'], .2)

    def test_slow_service_reports_lag_and_overload(self):
        with fixture('slow') as srv:
            p, r = load(srv.server_port, '--rps', '500', '--queue', '8', '--max-p99-ms', '10')
            self.assertNotEqual(p.returncode, 0, p.stdout)
            self.assertGreater(r['dropped'], 0)
            self.assertEqual(r['attempted'] + r['dropped'], r['planned'])
            self.assertGreater(r['max_ms'], 30)
            self.assertGreater(r['max_send_lag_ms'], 5)

    def test_exact_small_count_and_full_duration(self):
        with server('--ephemeral') as (port, _):
            p, r = load(port, '--rps', '1', '--duration', '1', '--connections', '4', '--pipeline', '128')
            self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
            self.assertEqual(r['planned'], 1)
            self.assertEqual(r['attempted'], 1)
            self.assertEqual(r['ok'], 1)
            self.assertGreaterEqual(r['elapsed_s'], .2)

    def test_randomized_deep_pipeline_counts(self):
        with server('--ephemeral') as (port, _):
            p, r = load(port, '--rps', '15005', '--duration', '1', '--seed', '100', '--connections', '3', '--pipeline', '512')
            self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
            self.assertEqual(r['ok'], 15005)
            self.assertEqual(r['latency_kind'], 'scheduled batch completion')

    def test_pipeline_latency_is_full_batch_completion(self):
        with fixture('slow') as srv:
            _, r = load(srv.server_port, '--rps', '40', '--pipeline', '8')
            self.assertEqual(r['attempted'], 8)
            self.assertGreater(r['p99_ms'], 100)

    def test_invalid_configuration_exits_cleanly(self):
        for args in [('--seed','0'), ('--duration','NaN'), ('--rps','0'), ('--pipeline','2048'), ('--pipeline','128','--write-ratio','.05')]:
            p = subprocess.run([str(BIN/'loadgen'), *args], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(p.returncode, 0)
            self.assertNotIn('panicked', p.stderr)

    def test_acknowledged_write_survives_sigkill(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp)/'durable.db')
            with server('--db', path) as (port, p):
                conn = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
                conn.request('POST', '/api/shorten', json.dumps({'url':'https://example.com/durable'}), {'Authorization':f'Bearer {KEY}', 'Content-Type':'application/json'})
                res = conn.getresponse(); self.assertEqual(res.status, 201)
                code = json.loads(res.read())['code']; conn.close()
                p.kill(); p.wait()
            with server('--db', path) as (port, _):
                conn = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
                conn.request('GET', '/'+code); res = conn.getresponse()
                self.assertEqual(res.status, 302)
                self.assertEqual(res.getheader('Location'), 'https://example.com/durable')
                res.read(); conn.close()

if __name__ == '__main__': unittest.main(verbosity=2)
