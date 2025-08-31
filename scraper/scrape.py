import argparse
import asyncio
import os
import subprocess
import sys

from sources.gmaps import run_gmaps

OUT_ROOT = os.getenv("OUT_ROOT") or os.path.abspath("out")


def main():
    parser = argparse.ArgumentParser(description="Run directory scrapers.")
    parser.add_argument("--source", choices=["yelp", "gmaps"], required=True)
    parser.add_argument("--q", required=True, help="search term, e.g. 'plumbers'")
    parser.add_argument("--location", required=True, help='e.g. "Austin, TX"')
    parser.add_argument("--limit", type=int)
    parser.add_argument("--use_proxy", type=int, default=0)
    parser.add_argument("--proxy_mode", choices=["rotating", "sticky"], default="rotating")
    parser.add_argument("--delay_min", type=float)
    parser.add_argument("--delay_max", type=float)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--outfile", help="(legacy) output JSONL file", default=None)
    parser.add_argument("--verify_concurrency", type=int, default=3)
    parser.add_argument("--max_pages", type=int, default=20)
    parser.add_argument("--max_retries", type=int, default=2,
                        help="Max retry passes for failed hrefs (default=2)")
    parser.add_argument("--retry_backoff", type=float, default=1.6,
                        help="Exponential backoff factor between retries (default=1.6)")
    parser.add_argument("--run_id", type=str, default=None,
                        help="Optional custom run id (otherwise auto-generated)")
    parser.add_argument("--ip_per_worker", type=int, default=0,
                        help="If set to 1, assign a unique proxy/IP per worker (up to concurrency). 0 = shared IP.")
    parser.add_argument("--export_csv", type=int, default=1, help="Export CSV after run (default=1)")
    parser.add_argument("--copy_to_run_dir", type=int, default=1, help="Copy results into run_dir (default=1)")
    parser.add_argument("--use_hunter_cache", type=int, default=1,
                        help="1=use cache for hits; 0=verify all fresh (cache still updated)")
    args = parser.parse_args()

    safe_q = args.q.replace(" ", "_")
    safe_loc_norm = args.location.replace(" ", "_").replace(",", "").lower()

    if not args.outfile:
        args.outfile = os.path.join(OUT_ROOT, f"{args.source}_{safe_q}_{safe_loc_norm}.jsonl")

    if args.source == "gmaps":
        asyncio.run(run_gmaps(args))
    else:
        print(f"[ERROR] this source is currently not supported: {args.source}")

    verify_script = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "tools", "scrape_verify_only.py")
    )

    gmaps_jsonl_for_verifier = os.path.join(OUT_ROOT, f"gmaps_{safe_q}_{safe_loc_norm}.jsonl")
    gmaps_jsonl_for_verifier_abs = os.path.abspath(gmaps_jsonl_for_verifier)

    if not os.path.exists(gmaps_jsonl_for_verifier_abs):
        print(f"[ERROR] Expected infile for verifier not found: {gmaps_jsonl_for_verifier_abs}")
        raise SystemExit(1)

    verify_outfile = os.path.join(OUT_ROOT, f"emails_{safe_q}_{safe_loc_norm}.jsonl")
    verify_outfile_abs = os.path.abspath(verify_outfile)
    os.makedirs(os.path.dirname(verify_outfile_abs), exist_ok=True)

    print(f"\n[INFO] triggering scrape_verify_only.py on {gmaps_jsonl_for_verifier_abs} ...")
    print(f"[INFO] writing email results to {verify_outfile_abs}\n")

    scraper_dir = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(scraper_dir, ".."))
    env = os.environ.copy()

    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join(
        [scraper_dir, project_root] + ([existing_pp] if existing_pp else [])
    )

    run_registry_dir = os.path.join(OUT_ROOT, "runs")

    print(f"[DEBUG] launching verifier with infile={gmaps_jsonl_for_verifier_abs}")
    print(f"[DEBUG] launching verifier with out={verify_outfile_abs}")
    print(f"[DEBUG] launching verifier with run_id={args.run_id} run_registry_dir={args.run_registry_dir}")

    cmd = [
        sys.executable,
        verify_script,
        "--infile", gmaps_jsonl_for_verifier_abs,
        "--out", verify_outfile_abs,
        "--verify-concurrency", str(args.verify_concurrency or 3),
        "--max-pages", str(args.max_pages or 20),
        "--site-concurrency", str(args.concurrency or 6),
        "--use-hunter-cache", str(args.use_hunter_cache or 0),
        "--run-registry-dir", run_registry_dir,
    ]
    if args.run_id:
        cmd.extend(["--run-id", args.run_id])

    subprocess.run(cmd, check=True, env=env)


if __name__ == "__main__":
    main()
