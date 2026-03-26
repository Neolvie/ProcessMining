"""
Integration tests against the running Process Mining API (http://localhost:8001).
Run: python tests/test_api.py
"""
import sys
import json
import urllib.request
import urllib.error

BASE = "http://localhost:8001"
PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
results = {"ok": 0, "fail": 0}


def get(path, params=""):
    url = BASE + "/api" + path + (f"?{params}" if params else "")
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


def check(name, condition, detail=""):
    if condition:
        print(f"  {PASS} {name}")
        results["ok"] += 1
    else:
        print(f"  {FAIL} {name}" + (f": {detail}" if detail else ""))
        results["fail"] += 1


print("\n── /api/status ──────────────────────────────────────────────────────")
code, d = get("/status")
check("HTTP 200", code == 200)
check("status = ready", d.get("status") == "ready", d.get("status"))
check("total_seconds present", d.get("total_seconds") is not None)
check("ai_enabled present", "ai_enabled" in d)
check("ai_enabled = True (env key set)", d.get("ai_enabled") is True, "check .env has OPENROUTER_API_KEY")
check("meta.parsed_events > 0", (d.get("meta") or {}).get("parsed_events", 0) > 0)

print("\n── /api/overview ────────────────────────────────────────────────────")
code, d = get("/overview")
check("HTTP 200", code == 200)
check("instances.total > 0", (d.get("instances") or {}).get("total", 0) > 0)
check("instances.completion_rate 0–100", 0 <= (d.get("instances") or {}).get("completion_rate", -1) <= 100)
check("performance.avg_duration_sec present", "avg_duration_sec" in (d.get("performance") or {}))
check("blocks.activations > 0", (d.get("blocks") or {}).get("activations", 0) > 0)
check("errors section present", "errors" in d)
check("unique_process_types > 0", d.get("unique_process_types", 0) > 0)

print("\n── /api/processes ───────────────────────────────────────────────────")
code, procs = get("/processes")
check("HTTP 200", code == 200)
check("returns list", isinstance(procs, list))
check("count > 0", len(procs) > 0, f"got {len(procs)}")
if procs:
    p = procs[0]
    check("process_id present", "process_id" in p)
    check("total > 0", p.get("total", 0) > 0)
    check("completion_rate 0–100", 0 <= p.get("completion_rate", -1) <= 100)
    check("abort_rate 0–100", 0 <= p.get("abort_rate", -1) <= 100)
    # Test process with quotes in name
    quoted = [x for x in procs if '"' in (x.get("display_name") or "")]
    check("process with quotes in name exists", len(quoted) > 0, "needed for quote-safety test")

print("\n── /api/process/{id} ────────────────────────────────────────────────")
if procs:
    pid = procs[0]["process_id"]
    code, d = get(f"/process/{pid}")
    check("HTTP 200", code == 200)
    check("instances list present", isinstance(d.get("instances"), list))
    check("block_stats list present", isinstance(d.get("block_stats"), list))
    check("instances count > 0", len(d.get("instances", [])) > 0)
    if d.get("instances"):
        inst = d["instances"][0]
        check("instance has instance_id", "instance_id" in inst)
        check("instance has status", inst.get("status") in ("completed","aborted","in_progress"))

    # Test process with quotes in name
    if procs:
        quoted = [x for x in procs if '"' in (x.get("display_name") or "")]
        if quoted:
            qpid = quoted[0]["process_id"]
            qname = quoted[0].get("display_name","")
            code2, d2 = get(f"/process/{qpid}")
            check(f"process with quotes opens OK: {qname[:40]}", code2 == 200)
            check("has instances", len(d2.get("instances", [])) > 0,
                  f"got {len(d2.get('instances',[]))} instances")

print("\n── /api/process/{id}/timeline ───────────────────────────────────────")
if procs:
    pid = procs[0]["process_id"]
    code, tl = get(f"/process/{pid}/timeline")
    check("HTTP 200", code == 200)
    check("returns list", isinstance(tl, list))
    if tl:
        check("hour field present", "hour" in tl[0])

print("\n── /api/timeline ────────────────────────────────────────────────────")
code, tl = get("/timeline", "granularity=hour")
check("HTTP 200 (hour)", code == 200)
check("returns list", isinstance(tl, list))
check("count > 0", len(tl) > 0, f"got {len(tl)}")
if tl:
    check("bucket field present", "bucket" in tl[0])
    check("process_starts field", "process_starts" in tl[0])

code, tl = get("/timeline", "granularity=day")
check("HTTP 200 (day)", code == 200)
check("day returns fewer rows", len(tl) < 200)

print("\n── /api/blocks ──────────────────────────────────────────────────────")
code, blocks = get("/blocks")
check("HTTP 200", code == 200)
check("returns list", isinstance(blocks, list))
check("count > 0", len(blocks) > 0)
if blocks:
    b = blocks[0]
    check("block_id present", "block_id" in b)
    check("activations > 0", b.get("activations", 0) > 0)
    check("avg_duration_sec numeric", isinstance(b.get("avg_duration_sec"), (int, float, type(None))))

print("\n── /api/flow ────────────────────────────────────────────────────────")
code, flow = get("/flow")
check("HTTP 200", code == 200)
check("returns list", isinstance(flow, list))
if flow:
    check("from_block present", "from_block" in flow[0])
    check("transition_count > 0", flow[0].get("transition_count", 0) > 0)

print("\n── /api/bottlenecks ─────────────────────────────────────────────────")
code, bn = get("/bottlenecks")
check("HTTP 200", code == 200)
check("slow_blocks list", isinstance(bn.get("slow_blocks"), list))
check("slow_processes list", isinstance(bn.get("slow_processes"), list))
check("high_abort_blocks list", isinstance(bn.get("high_abort_blocks"), list))

print("\n── /api/issues ──────────────────────────────────────────────────────")
code, iss = get("/issues")
check("HTTP 200", code == 200)
check("abort_by_process list", isinstance(iss.get("abort_by_process"), list))
check("long_running_instances list", isinstance(iss.get("long_running_instances"), list))
check("failed_spans list", isinstance(iss.get("failed_spans"), list))

print("\n── /api/heatmap ─────────────────────────────────────────────────────")
code, hm = get("/heatmap")
check("HTTP 200", code == 200)
check("returns list", isinstance(hm, list))
check("count > 0", len(hm) > 0)
if hm:
    check("dow field 0–6", 0 <= hm[0].get("dow", -1) <= 6)
    check("hour field 0–23", 0 <= hm[0].get("hour", -1) <= 23)
    check("starts field present", "starts" in hm[0])

print("\n── /api/histogram ───────────────────────────────────────────────────")
code, hist = get("/histogram")
check("HTTP 200", code == 200)
check("returns list", isinstance(hist, list))
check("count > 0", len(hist) > 0, f"got {len(hist)}")
if hist:
    check("bucket field present", "bucket" in hist[0])
    check("count field present", "count" in hist[0])

print("\n── /api/filters ─────────────────────────────────────────────────────")
code, flt = get("/filters")
check("HTTP 200", code == 200)

print("\n── /api/ai-insights (SSE) ───────────────────────────────────────────")
import urllib.request
try:
    req = urllib.request.Request(BASE + "/api/ai-insights")
    with urllib.request.urlopen(req, timeout=30) as r:
        code = r.status
        chunk = r.read(512).decode("utf-8", errors="replace")
    check("HTTP 200", code == 200)
    check("SSE data: prefix", "data:" in chunk, repr(chunk[:100]))
    check("not error response", "[DONE]" in chunk or '"text"' in chunk, repr(chunk[:200]))
except Exception as e:
    check("HTTP 200", False, str(e))

print(f"\n{'─'*60}")
total = results['ok'] + results['fail']
print(f"Results: {results['ok']}/{total} passed", "✓" if results['fail'] == 0 else f"  ({results['fail']} FAILED)")
sys.exit(0 if results['fail'] == 0 else 1)
