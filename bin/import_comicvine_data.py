#!/usr/bin/env python3
"""
Import full Marvel data from Comic Vine API
Requires free API key from https://comicvine.gamespot.com/api/
Rate limited to 200 requests/hour

Supports automatic checkpointing -- if interrupted, re-running resumes
from where it left off. Use --no-resume to force a fresh start.
"""

import json
import csv
import gzip
import os
import requests
from time import sleep, time
from pathlib import Path
from collections import defaultdict

API_BASE = "https://comicvine.gamespot.com/api"
CHECKPOINT_FILE = "data/.comicvine_checkpoint.json"
CHECKPOINT_INTERVAL = 50  # Save every N entities (slower API, save more often)

class RateLimiter:
    """Rate limiter: 200 requests per hour"""
    def __init__(self, requests_per_hour=200):
        self.requests_per_hour = requests_per_hour
        self.min_interval = 3600 / requests_per_hour  # seconds between requests
        self.last_request = 0
        self.request_count = 0
        self.hour_start = time()

    def wait(self):
        """Wait if necessary to respect rate limit"""
        now = time()

        # Reset counter every hour
        if now - self.hour_start >= 3600:
            self.request_count = 0
            self.hour_start = now

        # Wait minimum interval between requests
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            print(f"  [Rate limit] Waiting {sleep_time:.1f}s...")
            sleep(sleep_time)

        self.last_request = time()
        self.request_count += 1

        if self.request_count >= self.requests_per_hour:
            wait_time = 3600 - (time() - self.hour_start)
            if wait_time > 0:
                print(f"  [Rate limit] Hit hourly limit. Waiting {wait_time/60:.1f} minutes...")
                sleep(wait_time)
                self.request_count = 0
                self.hour_start = time()

limiter = RateLimiter()

def api_get(endpoint, params=None):
    """Make API request with rate limiting"""
    limiter.wait()

    if params is None:
        params = {}

    params['format'] = 'json'

    url = f"{API_BASE}/{endpoint}/"
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    if data.get('status_code') != 1:
        raise Exception(f"API error: {data.get('error')}")

    return data['results']

# ─── Checkpoint helpers ──────────────────────────────────────────────────────

def _checkpoint_path(prefix):
    return CHECKPOINT_FILE.replace('.comicvine', f'.comicvine_{prefix}')

def save_checkpoint(entity_issues, prefix):
    """Atomically save progress checkpoint"""
    Path("data").mkdir(exist_ok=True)
    checkpoint_file = _checkpoint_path(prefix)
    tmp = checkpoint_file + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(entity_issues, f)
    os.replace(tmp, checkpoint_file)

def load_checkpoint(prefix):
    """Load existing checkpoint, or return empty dict"""
    path = Path(_checkpoint_path(prefix))
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"[Checkpoint] Resuming {prefix} -- {len(data)} entities already fetched")
    return data

def clear_checkpoint(prefix):
    """Remove checkpoint after successful completion"""
    path = Path(_checkpoint_path(prefix))
    if path.exists():
        path.unlink()
        print(f"[Checkpoint] Cleared {prefix} checkpoint after successful completion")

# ─── API fetchers ────────────────────────────────────────────────────────────

def fetch_marvel_characters(api_key):
    """Fetch all Marvel characters"""
    print("Fetching Marvel characters...")
    characters = []
    offset = 0
    limit = 100

    while True:
        results = api_get("characters", {
            "api_key": api_key,
            "filter": "publisher:31",  # Marvel is publisher ID 31
            "limit": limit,
            "offset": offset,
            "field_list": "id,name,deck,image,site_detail_url,character_issues"
        })

        if not results:
            break

        characters.extend(results)
        print(f"  Fetched {len(characters)} characters")
        offset += limit

        if len(results) < limit:
            break

    return characters

def fetch_marvel_creators(api_key):
    """Fetch all Marvel creators"""
    print("Fetching Marvel creators...")
    creators = []
    offset = 0
    limit = 100

    while True:
        results = api_get("people", {
            "api_key": api_key,
            "limit": limit,
            "offset": offset,
            "field_list": "id,name,deck,image,site_detail_url"
        })

        if not results:
            break

        creators.extend(results)
        print(f"  Fetched {len(creators)} creators")
        offset += limit

        if len(results) < limit:
            break

    return creators

def fetch_character_issues(api_key, character_id):
    """Fetch issues for a character"""
    results = api_get("issues", {
        "api_key": api_key,
        "filter": f"character:{character_id}",
        "limit": 100,
        "field_list": "id,name,issue_number,volume,cover_date,image,site_detail_url"
    })
    return results

# ─── Graph builders ──────────────────────────────────────────────────────────

def build_characters_graph(api_key, characters, max_chars=None, no_resume=False):
    """Build character co-occurrence network, with checkpoint support"""
    print("\nBuilding character network...")

    if max_chars:
        characters = characters[:max_chars]

    # Load existing checkpoint unless --no-resume was passed
    char_issues = {} if no_resume else load_checkpoint("characters")
    already_done = set(char_issues.keys())  # string-keyed from JSON

    for i, char in enumerate(characters):
        char_id_str = str(char['id'])

        if char_id_str in already_done:
            print(f"[{i+1}/{len(characters)}] Already have {char['name']} -- skipping")
            continue

        print(f"[{i+1}/{len(characters)}] Processing {char['name']}...")
        try:
            issues = fetch_character_issues(api_key, char['id'])
            char_issues[char_id_str] = {
                'info': char,
                'issues': [iss['id'] for iss in issues]
            }
        except Exception as e:
            print(f"  Error: {e}")
            continue

        if (i + 1) % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(char_issues, "characters")

    # Build nodes
    nodes = []
    for char_id_str, data in char_issues.items():
        char = data['info']
        issues = data['issues']

        image_url = ""
        if char.get('image') and char['image'].get('medium_url'):
            image_url = char['image']['medium_url']

        node = {
            "key": char_id_str,
            "attributes": {
                "label": char['name'],
                "image": "",
                "image_url": image_url,
                "url": char.get('site_detail_url', ''),
                "stories": len(issues),
                "description": char.get('deck', ''),
                "x": 0,
                "y": 0,
                "size": None,
                "community": 0,
                "source": "comicvine",
                "comicvine_id": char['id']
            }
        }
        nodes.append(node)

    # Build edges (co-occurrence on same issues)
    print("\nBuilding co-occurrence edges...")
    issue_to_chars = defaultdict(list)
    for char_id_str, data in char_issues.items():
        for issue_id in data['issues']:
            issue_to_chars[issue_id].append(char_id_str)

    edges = {}
    for issue_id, char_ids in issue_to_chars.items():
        for i, c1 in enumerate(char_ids):
            for c2 in char_ids[i+1:]:
                edge_key = tuple(sorted([c1, c2]))
                if edge_key not in edges:
                    edges[edge_key] = 0
                edges[edge_key] += 1

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

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Import data from Comic Vine API')
    parser.add_argument('--api-key', required=True, help='Comic Vine API key')
    parser.add_argument('--characters-only', action='store_true', help='Only import characters')
    parser.add_argument('--creators-only', action='store_true', help='Only import creators')
    parser.add_argument('--max-chars', type=int, help='Limit characters (for testing)')
    parser.add_argument('--no-resume', action='store_true', help='Ignore existing checkpoint and start fresh')
    args = parser.parse_args()

    if not args.creators_only:
        print("=" * 60)
        print("IMPORTING CHARACTERS")
        print("=" * 60)

        characters = fetch_marvel_characters(args.api_key)
        graph = build_characters_graph(
            args.api_key,
            characters,
            args.max_chars,
            no_resume=args.no_resume
        )

        output_file = "data/comicvine_characters.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(graph, f, indent=2)

        clear_checkpoint("characters")

        print(f"\n[OK] Characters import complete!")
        print(f"  Characters: {len(graph['nodes'])}")
        print(f"  Edges: {len(graph['edges'])}")
        print(f"  Output: {output_file}")

    if not args.characters_only:
        print("\n" + "=" * 60)
        print("IMPORTING CREATORS")
        print("=" * 60)

        creators = fetch_marvel_creators(args.api_key)

        print(f"\n[OK] Creators fetch complete!")
        print(f"  Creators: {len(creators)}")
        print("  Note: Building creator network requires fetching their issues")
        print("        Run with --creators-only to build full network")

    print("\n" + "=" * 60)
    print("RATE LIMIT STATUS")
    print("=" * 60)
    print(f"Requests made: {limiter.request_count}/{limiter.requests_per_hour}")

if __name__ == "__main__":
    main()
