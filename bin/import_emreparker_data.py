#!/usr/bin/env python3
"""
Import creators and comics from emreparker Marvel API
No API key required!
Data goes up to 2024!

Idempotent by API index:
  Re-running only fetches creator IDs not already in the raw cache.
  The raw cache (RAW_CACHE_FILE) persists between runs as the source silo.
  A separate progress file (PROGRESS_FILE) handles crash recovery mid-run.

Run again any time — only new creators will be fetched.
Use --no-resume to ignore the cache and fetch everything fresh.
"""

import json
import csv
import gzip
import os
import requests
from time import sleep
from pathlib import Path

API_BASE = "https://marvel.emreparker.com/v1"
RATE_LIMIT_DELAY = 1.0  # 60 requests/minute = 1 per second

# Persistent raw cache — survives between runs; grows incrementally
RAW_CACHE_FILE = "data/emreparker_raw_cache.json"
# In-progress crash recovery file — deleted on clean completion
PROGRESS_FILE  = "data/.emreparker_progress.json"
CHECKPOINT_INTERVAL = 100  # Save progress every N new creators

def api_get(endpoint, params=None, retries=3):
    """Make API request with rate limiting and retry logic"""
    url = f"{API_BASE}/{endpoint}"

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            sleep(RATE_LIMIT_DELAY)
            return response.json()
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff: 5s, 10s, 15s
                print(f"  [Retry {attempt+1}/{retries}] Error: {type(e).__name__}. Waiting {wait_time}s...")
                sleep(wait_time)
            else:
                print(f"  [Failed] Could not fetch {endpoint} after {retries} attempts: {e}")
                raise

    return None

# ─── Cache / progress helpers ─────────────────────────────────────────────────

def _atomic_write(data, path):
    """Write JSON atomically via a temp file so a crash mid-write corrupts nothing"""
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    os.replace(tmp, path)

def load_raw_cache():
    """Load the persistent raw cache (data fetched in all previous runs)"""
    path = Path(RAW_CACHE_FILE)
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"[Cache] Loaded {len(data)} creators from previous runs")
    return data

def save_raw_cache(creator_issues):
    """Persist the full raw cache so future runs skip already-fetched IDs"""
    Path("data").mkdir(exist_ok=True)
    _atomic_write(creator_issues, RAW_CACHE_FILE)

def save_progress(creator_issues):
    """Save mid-run progress for crash recovery (separate from persistent cache)"""
    _atomic_write(creator_issues, PROGRESS_FILE)

def load_progress():
    """Load in-progress crash recovery file if it exists"""
    path = Path(PROGRESS_FILE)
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"[Progress] Found crash-recovery file with {len(data)} creators")
    return data

def clear_progress():
    """Remove the in-progress file after a clean run"""
    path = Path(PROGRESS_FILE)
    if path.exists():
        path.unlink()

# ─── API fetchers ─────────────────────────────────────────────────────────────

def fetch_all_creators():
    """Fetch all creators from the API"""
    print("Fetching creators...")
    creators = []
    offset = 0
    limit = 100

    while True:
        data = api_get("creators", {"limit": limit, "offset": offset})
        creators.extend(data['items'])
        print(f"  Fetched {len(creators)}/{data['total']} creators")

        if not data.get('has_next'):
            break
        offset += limit

    return creators

def fetch_creator_issues(creator_id, creator_name):
    """Fetch all issues for a creator"""
    print(f"  Fetching issues for {creator_name}...")
    issues = []
    offset = 0
    limit = 100

    while True:
        try:
            data = api_get(f"creators/{creator_id}/issues", {"limit": limit, "offset": offset})
            if not data:
                break
            issues.extend(data['items'])

            if not data.get('has_next'):
                break
            offset += limit
        except Exception as e:
            print(f"  WARNING: Skipping {creator_name} due to error: {e}")
            break

    return issues

# ─── Graph builder ───────────────────────────────────────────────────────────

def build_creators_graph(creators, max_creators=None, start_from=0, no_resume=False):
    """Build creators co-occurrence network.

    Idempotent: loads the persistent raw cache first, then only fetches
    creator IDs not already present.  The cache is updated and saved at
    the end of every successful run, so the next run sees all prior data.
    """
    print("\nBuilding creators network...")

    if max_creators:
        creators = creators[:max_creators]

    if no_resume:
        creator_issues = {}
    else:
        # Layer 1: persistent raw cache from all previous runs
        creator_issues = load_raw_cache()
        # Layer 2: in-progress crash-recovery file from this run (if any)
        progress = load_progress()
        creator_issues.update(progress)   # progress wins over stale cache entries

    already_done = set(creator_issues.keys())  # string keys (from JSON)
    skipped = []
    new_count = 0

    for i, creator in enumerate(creators[start_from:], start=start_from):
        creator_id_str = str(creator['id'])

        # Skip creators we already have — idempotent by API index
        if creator_id_str in already_done:
            continue

        print(f"[{i+1}/{len(creators)}] Processing {creator['name']}...")
        try:
            issues = fetch_creator_issues(creator['id'], creator['name'])
            creator_issues[creator_id_str] = {
                'info': creator,
                'issues': issues
            }
            new_count += 1
        except Exception as e:
            print(f"  ERROR: Skipping {creator['name']}: {e}")
            skipped.append(creator['name'])
            continue

        # Save mid-run progress for crash recovery every CHECKPOINT_INTERVAL new creators
        if new_count % CHECKPOINT_INTERVAL == 0:
            save_progress(creator_issues)

    if new_count == 0:
        print(f"  No new creators found — cache is up to date ({len(already_done)} creators)")
    else:
        print(f"  Fetched {new_count} new creators")

    if skipped:
        print(f"\nSkipped {len(skipped)} creators due to errors")
        if len(skipped) <= 10:
            print("Skipped:", ", ".join(skipped))

    # Identify stub entries (seeded from a previous run's silo output, no raw issue data)
    stub_ids = {cid for cid, data in creator_issues.items() if data.get('issues') is None}
    if stub_ids:
        print(f"  {len(stub_ids)} cached entries are stubs (no raw issue data) — "
              f"will load edges from existing silo")

    # Build nodes
    nodes = []
    for creator_id_str, data in creator_issues.items():
        creator  = data['info']
        issues   = data['issues'] or []  # stubs have None

        writer_count = sum(1 for iss in issues if 'writer' in iss.get('role', '').lower())
        artist_count = len(issues) - writer_count

        node = {
            "key": creator_id_str,
            "attributes": {
                "label": creator['name'],
                "image": "",
                "image_url": "",
                "url": f"https://marvel.emreparker.com/creators/{creator.get('id', creator_id_str)}",
                "stories": len(issues),
                "writer": writer_count,
                "artist": artist_count,
                "x": 0,
                "y": 0,
                "size": None,
                "source": "emreparker_2024",
                "marvel_id": creator.get('id', creator_id_str)
            }
        }
        nodes.append(node)

    # Build edges (co-occurrence on same issues)
    # Only uses cache entries that have real issue data.
    print("\nBuilding co-occurrence edges...")
    issue_to_creators = {}
    for creator_id_str, data in creator_issues.items():
        if data.get('issues') is None:
            continue   # stub — no raw data
        for issue in data['issues']:
            issue_id = issue['id']
            if issue_id not in issue_to_creators:
                issue_to_creators[issue_id] = []
            issue_to_creators[issue_id].append(creator_id_str)

    edges = {}
    for issue_id, creator_ids in issue_to_creators.items():
        for i, c1 in enumerate(creator_ids):
            for c2 in creator_ids[i+1:]:
                edge_key = tuple(sorted([c1, c2]))
                if edge_key not in edges:
                    edges[edge_key] = 0
                edges[edge_key] += 1

    # If stubs exist, the new-creator edges above are incomplete.
    # Merge with existing silo edges so cross-edges to old creators are preserved.
    if stub_ids:
        silo_path = Path("data/emreparker_creators.json")
        if silo_path.exists():
            print("  Merging with existing silo edges to restore cross-edges...")
            existing = json.load(open(silo_path))
            for edge in existing.get('edges', []):
                ek = tuple(sorted([edge['source'], edge['target']]))
                if ek not in edges:   # add silo edge if not already rebuilt
                    edges[ek] = edge['attributes'].get('weight', 1)

    edges_list = [
        {
            "source": edge[0],
            "target": edge[1],
            "attributes": {"weight": weight}
        }
        for edge, weight in edges.items()
    ]

    return {
        "options": {
            "type": "undirected",
            "multi": False,
            "allowSelfLoops": False
        },
        "attributes": {},
        "nodes": nodes,
        "edges": edges_list
    }

def fetch_all_issues():
    """Fetch all issues for comics CSV"""
    print("\nFetching all issues...")
    all_issues = []

    # Fetch by year for efficiency
    for year in range(1939, 2025):
        print(f"  Fetching issues from {year}...")
        offset = 0
        limit = 100

        while True:
            data = api_get("issues", {"year": year, "limit": limit, "offset": offset})
            all_issues.extend(data['items'])

            if not data.get('has_next'):
                break
            offset += limit

        print(f"    Total so far: {len(all_issues)}")

    return all_issues

def build_comics_csv(issues, output_path):
    """Build comics CSV from issues"""
    print(f"\nBuilding comics CSV...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(output_path, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'title', 'date', 'description', 'characters', 'writers', 'artists', 'image_url', 'url'])

        for issue in issues:
            # Fetch full details for this issue
            full_issue = api_get(f"issues/{issue['id']}")

            # Extract creators by role
            creators = full_issue.get('creators', [])
            writers = '|'.join(str(c['id']) for c in creators if 'writer' in c.get('role', '').lower())
            artists = '|'.join(str(c['id']) for c in creators if 'writer' not in c.get('role', '').lower())

            # Build row
            cover = full_issue.get('cover', {})
            image_url = f"{cover.get('path', '')}.{cover.get('extension', 'jpg')}" if cover else ""

            writer.writerow([
                full_issue['id'],
                full_issue['title'],
                full_issue.get('onSaleDate', '')[:7] if full_issue.get('onSaleDate') else '',
                full_issue.get('description', ''),
                '',  # No character data in this API
                writers,
                artists,
                image_url,
                full_issue['detailUrl']
            ])

    print(f"Saved {len(issues)} issues to {output_path}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Import data from emreparker Marvel API')
    parser.add_argument('--creators-only', action='store_true', help='Only import creators network')
    parser.add_argument('--comics-only', action='store_true', help='Only import comics CSV')
    parser.add_argument('--max-creators', type=int, help='Limit number of creators (for testing)')
    parser.add_argument('--start-from', type=int, default=0, help='Resume from creator index (for recovery)')
    parser.add_argument('--no-resume', action='store_true', help='Ignore existing checkpoint and start fresh')
    args = parser.parse_args()

    if not args.comics_only:
        # Import creators
        creators = fetch_all_creators()
        graph = build_creators_graph(
            creators,
            args.max_creators,
            args.start_from,
            no_resume=args.no_resume
        )

        output_file = "data/emreparker_creators.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(graph, f, indent=2)

        # Persist the full raw cache so future runs know what was already fetched
        # (must happen AFTER the output JSON is written successfully)
        save_raw_cache(creator_issues)
        clear_progress()

        print(f"\n[OK] Creators import complete!")
        print(f"  Creators: {len(graph['nodes'])}")
        print(f"  Edges: {len(graph['edges'])}")
        print(f"  Output: {output_file}")

    if not args.creators_only:
        # Import comics
        issues = fetch_all_issues()
        build_comics_csv(issues, "data/emreparker_comics.csv.gz")

        print(f"\n[OK] Comics import complete!")

if __name__ == "__main__":
    main()
