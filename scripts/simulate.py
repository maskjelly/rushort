#!/usr/bin/env python3
"""Real HTTP workload simulation. No extrapolated daily totals are reported as measured traffic."""
import argparse
import contextlib
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import secrets
import socket
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]

def command(args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

@contextlib.contextmanager
def serve(out, label, durable):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    # Isolated process, isolated data, isolated credentials; never attach to an existing listener.
    env = dict(os.environ, RUSHORT_API_KEY=secrets.token_hex(32))
    args = [str(ROOT/'target/release/shortener'), '--bind', f'127.0.0.1:{port}', '--max-urls', '1000000']
    args += ['--db', str(out/(label+'.db'))] if durable else ['--ephemeral']
    with (out/(label+'-server.log')).open('w') as log:
        process = subprocess.Popen(args, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(300):
                if process.poll() is not None: raise RuntimeError('owned server exited; inspect server log')
                try:
                    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=.2)
                    conn.request('GET', '/ready'); res = conn.getresponse()
                    ok = res.status == 200; res.read(); conn.close()
                    if ok: break
                except OSError: pass
                time.sleep(.02)
            else: raise RuntimeError('server readiness timeout')
            yield f'http://127.0.0.1:{port}', env
        finally:
            process.terminate()
            try: process.wait(timeout=12)
            except subprocess.TimeoutExpired: process.kill(); process.wait()

def run(out, name, target, env, flags):
    args = [str(ROOT/'target/release/loadgen'), '--target', target, '--json', str(out/(name+'.json')), *map(str, flags)]
    print(f'{name}: running', flush=True)
    with (out/(name+'.log')).open('w') as log:
        result = subprocess.run(args, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=float(flags[flags.index('--duration')+1])+1800)
    path = out/(name+'.json')
    data = json.loads(path.read_text()) if path.exists() else {'pass':False, 'error':'no report; see log'}
    data.update(name=name, command=args, exit_code=result.returncode)
    if result.returncode: data['pass'] = False
    print(f"{name}: {'PASS' if data['pass'] else 'FAIL'}; {data.get('rps',0):,.0f} RPS; p99 {data.get('p99_ms',0):.2f} ms; dropped {data.get('dropped',0)}", flush=True)
    return data

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', default=None)
    p.add_argument('--seconds', type=int, default=30, help='durable average-rate observation window')
    p.add_argument('--repeats', type=int, default=3, help='repeat both saturation workloads')
    p.add_argument('--soak-seconds', type=int, default=0, help='additional durable 1,158 RPS soak, up to 3600 seconds at the default capacity')
    a = p.parse_args()
    if a.seconds < 1 or not 1 <= a.repeats <= 10 or not 0 <= a.soak_seconds <= 3600: p.error('invalid duration/repeats')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = Path(a.out).resolve() if a.out else ROOT/'target'/'benchmarks'/stamp
    out.mkdir(parents=True, exist_ok=False)
    sources = sorted([ROOT/'Cargo.toml', ROOT/'Cargo.lock', *ROOT.glob('src/**/*.rs')])
    digest = hashlib.sha256()
    for f in sources: digest.update(str(f.relative_to(ROOT)).encode()+b'\0'+f.read_bytes())
    metadata = dict(utc=stamp, platform=platform.platform(), cpu_count=os.cpu_count(), rust=command(['rustc','-Vv']), revision=command(['git','rev-parse','HEAD']), dirty=bool(command(['git','status','--porcelain'])), source_sha256=digest.hexdigest(), transport='HTTP/1.1 loopback; client and server on same machine', build='cargo build --release --locked', scenarios=[])
    metadata['diff']=command(['git','diff','--stat'])
    if sys.platform=='darwin': metadata['cpu']=command(['sysctl','-n','machdep.cpu.brand_string'])
    results=metadata['scenarios']
    with serve(out,'durable',True) as (target,env):
        common=['--connections',64,'--seed',1000,'--write-ratio',.05,'--miss-ratio',.01,'--max-p99-ms',50]
        results.append(run(out,'durable-daily-average',target,env,['--rps',1158,'--duration',a.seconds,*common]))
        results.append(run(out,'durable-2x-burst',target,env,['--rps',2316,'--duration',10,*common]))
        if a.soak_seconds:
            results.append(run(out,'durable-soak',target,env,['--rps',1158,'--duration',a.soak_seconds,*common]))
    with serve(out,'ephemeral-100M',False) as (target,env):
        results.append(run(out,'pipeline-100M-in-10s',target,env,['--rps',10000000,'--duration',10,'--connections',32,'--pipeline',128,'--queue',128,'--seed',1000]))
    with serve(out,'ephemeral',False) as (target,env):
        for n in range(a.repeats):
            results.append(run(out,f'roundtrip-{n+1}',target,env,['--mode','saturate','--duration',5,'--connections',64,'--seed',1000]))
            results.append(run(out,f'pipeline-peak-{n+1}',target,env,['--mode','saturate','--duration',5,'--connections',32,'--pipeline',128,'--seed',1000]))
        # Larger working set plus misses exercises lookup locality and response ordering.
        results.append(run(out,'pipeline-wide-mixed',target,env,['--mode','saturate','--duration',5,'--connections',32,'--pipeline',128,'--seed',100000,'--miss-ratio',.1]))
    (out/'summary.json').write_text(json.dumps(metadata,indent=2)+'\n')
    lines=['# Measured claims', '', f'Run {stamp}; source SHA-256 `{metadata["source_sha256"]}`.', '', 'Client and server shared one machine over HTTP loopback. No TLS or NIC. Ephemeral and durable results are separate.', '', '| Scenario | Pass | RPS | Successful requests | Actual seconds | p99 ms |', '|---|---|---:|---:|---:|---:|']
    for r in results: lines.append(f'| {r["name"]} | {r["pass"]} | {r.get("rps",0):,.0f} | {r.get("ok",0):,} | {r.get("elapsed_s",0):.3f} | {r.get("p99_ms",0):.3f} |')
    daily=results[0]
    lines += ['', 'Pipeline p99 is full batch completion; rate-mode latency includes scheduling/queue delay. Saturation latency starts at actual dispatch. All recorded errors and drops fail a run.']
    if daily['pass']:
        lines += ['', f'Durable mixed traffic sustained {daily["rps"]:,.0f} RPS for {daily["elapsed_s"]:.1f}s: 94% redirect GET, 5% authenticated POST, 1% missing GET target mix. Each acknowledged write was checked afterward.', '', '100 million/day averages 1,157.41 RPS. This short run tests that arrival rate; it is not a 24-hour or 100-million-request durability test.']
    claim=next(r for r in results if r['name']=='pipeline-100M-in-10s')
    if claim['pass']:
        lines += ['', f'Quote: "My Rust URL shortener processed {claim["ok"]:,} randomized redirect GETs in {claim["elapsed_s"]:.3f} seconds on localhost, using 32 connections with pipeline depth 128. In-memory processing benchmark; durable mixed traffic measured separately."']
    else: lines += ['', 'The 100M-in-10s claim did not pass. Do not publish that claim from this build. See the recorded drops, errors, latency and achieved rate.']
    for prefix in ['roundtrip-','pipeline-peak-']:
        rates=[r['rps'] for r in results if r['name'].startswith(prefix) and r['pass']]
        if rates: lines += ['', f'{prefix} median {statistics.median(rates):,.0f} RPS; range {min(rates):,.0f}–{max(rates):,.0f}; n={len(rates)}.']
    (out/'CLAIMS.md').write_text('\n'.join(lines)+'\n')
    print(f'Results: {out}', flush=True)
    return 0 if all(r['pass'] for r in results) else 1

if __name__=='__main__': sys.exit(main())
