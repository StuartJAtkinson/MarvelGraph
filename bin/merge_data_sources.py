#!/usr/bin/env python3
"""
Merge data from multiple source silos into a single graph for spatialization.

Source silos (inputs — never overwritten by this script or the pipeline):
  Creators:
    data/emreparker_creators.json          primary, 1939-2024, raw co-occurrence counts
    data/Marvel_creators_2022_supplement.json  241 creators in 2022 data but not emreparker

  Characters:
    data/Marvel_characters_source_2022.json.gz  co-occurrence network from 2022 Marvel API
    data/superhero_characters.json              supplemental metadata (no edges)
    data/comicvine_characters.json              optional, requires API key

Web outputs (never used as merge inputs — written by process_imported_data.js):
    data/Marvel_creators_by_stories_full.json.gz   <- spatializer writes here
    data/Marvel_characters_by_stories_full.json.gz <- spatializer writes here

Merge strategy:
  - Creators are deduplicated by Marvel ID.
  - For numeric counts (stories/writer/artist) the SOURCE WITH THE HIGHER VALUE wins;
    counts are never summed across overlapping sources to avoid double-counting.
  - Other attributes: first non-empty value wins.
  - Edge weights from spatialized sources (stored as int*1000) are divided back to
    raw floats before being combined with raw co-occurrence counts.

Usage:
  python bin/merge_data_sources.py --creators --output data/Marvel_creators_merged.json
  python bin/merge_data_sources.py --characters --output data/Marvel_characters_merged.json
"""

import json
import zlib
from pathlib import Path
from collections import defaultdict


# ─── Loaders ─────────────────────────────────────────────────────────────────

def load_json(filepath):
    """Load a graphology JSON silo, supporting pako/zlib compression."""
    path = Path(filepath)
    if not path.exists():
        return None
    if filepath.endswith('.gz'):
        data = zlib.decompress(path.read_bytes())
        return json.loads(data)
    return json.loads(path.read_text(encoding='utf-8'))


# ─── Node merge ──────────────────────────────────────────────────────────────

def _merge_node_into(existing, attrs, source_name):
    """Merge attrs from a new source into an existing merged node.

    Counts: keep the maximum value (no summing — sources overlap chronologically).
    Other attrs: prefer non-empty value; existing value wins on ties.
    """
    ea = existing['attributes']

    for key, value in attrs.items():
        if key == 'sources':
            continue
        if key in ('stories', 'writer', 'artist', 'comics'):
            # Use the higher count — emreparker covers more history than 2022 data
            if value and value > ea.get(key, 0):
                ea[key] = value
        else:
            # Keep first non-empty value
            if not ea.get(key) and value:
                ea[key] = value

    if source_name not in ea.get('sources', []):
        ea.setdefault('sources', []).append(source_name)


def merge_nodes(sources_data, id_field=None):
    """
    Merge nodes from multiple sources.

    id_field: if set, use attrs[id_field] as dedup key instead of node['key'].
    Returns list of merged node dicts.
    """
    by_id   = {}   # Marvel ID (or other canonical ID) -> merged node
    by_name = {}   # normalised label -> merged node (fallback)

    for source_name, data in sources_data.items():
        if not data:
            continue

        print(f"  [{source_name}] {len(data['nodes'])} nodes")

        for node in data['nodes']:
            node_id = node['key']
            attrs   = node['attributes']
            name    = attrs.get('label', '').lower().strip()

            # Prefer the attribute-specified ID if given (e.g. marvel_id)
            canon_id = str(attrs[id_field]) if id_field and attrs.get(id_field) else node_id

            if canon_id in by_id:
                _merge_node_into(by_id[canon_id], attrs, source_name)
            elif name in by_name:
                _merge_node_into(by_name[name], attrs, source_name)
            else:
                merged = {
                    'key': canon_id,
                    'attributes': {**attrs, 'sources': [source_name]}
                }
                by_id[canon_id] = merged
                if name:
                    by_name[name] = merged

    # Return all unique nodes (by_name may share references with by_id, deduplicate)
    seen = set()
    result = []
    for node in list(by_id.values()) + list(by_name.values()):
        nid = id(node)
        if nid not in seen:
            seen.add(nid)
            result.append(node)

    return result


# ─── Edge merge ──────────────────────────────────────────────────────────────

def merge_edges(sources_data, spatialized_sources=None):
    """
    Merge edges from multiple sources.

    spatialized_sources: set of source names whose edge weights are stored
    as int*1000 (PMI-transformed, quantised).  These are divided back to
    raw float before combining with raw co-occurrence weights.
    """
    spatialized_sources = spatialized_sources or set()
    edges_map = {}

    for source_name, data in sources_data.items():
        if not data or 'edges' not in data:
            continue

        for edge in data['edges']:
            # Skip self-loops (bugs in some source data)
            if edge['source'] == edge['target']:
                continue

            key = tuple(sorted([edge['source'], edge['target']]))

            raw_weight = edge['attributes'].get('weight', 1)
            if source_name in spatialized_sources:
                # Undo quantisation: stored value was round(PMI * 1000)
                raw_weight = raw_weight / 1000.0

            if key not in edges_map:
                edges_map[key] = {
                    'source': edge['source'],
                    'target': edge['target'],
                    'attributes': {'weight': 0.0, 'sources': []}
                }

            edges_map[key]['attributes']['weight'] += raw_weight
            if source_name not in edges_map[key]['attributes']['sources']:
                edges_map[key]['attributes']['sources'].append(source_name)

    print(f"  Total edges after merge: {len(edges_map)}")
    return list(edges_map.values())


# ─── Merge routines ───────────────────────────────────────────────────────────

def merge_creators():
    """
    Merge creators from source silos.

    Sources (in priority order — higher story counts win):
      1. emreparker_creators.json       primary: raw co-occurrence, 1939-2024
      2. Marvel_creators_2022_supplement.json  241 creators absent from emreparker
    """
    print("=" * 60)
    print("MERGING CREATORS")
    print("=" * 60)

    sources = {
        'emreparker_2024':   load_json('data/emreparker_creators.json'),
        'marvel_2022_supp':  load_json('data/Marvel_creators_2022_supplement.json'),
    }

    for name, d in sources.items():
        if d is None:
            print(f"  WARNING: {name} silo not found — skipping")

    nodes = merge_nodes({k: v for k, v in sources.items() if v},
                        id_field='marvel_id')

    # Edges only from emreparker (raw co-occurrence counts, not PMI-quantised)
    edges = merge_edges(
        {'emreparker_2024': sources['emreparker_2024']},
        spatialized_sources=set()
    )

    return nodes, edges


def merge_characters():
    """
    Merge characters from source silos.

    Sources:
      1. Marvel_characters_source_2022.json.gz  co-occurrence network (spatialized storage)
      2. superhero_characters.json              supplemental metadata, no edges
      3. comicvine_characters.json              optional, raw co-occurrence counts
    """
    print("=" * 60)
    print("MERGING CHARACTERS")
    print("=" * 60)

    sources = {
        'marvel_2022':   load_json('data/Marvel_characters_source_2022.json.gz'),
        'superhero_api': load_json('data/superhero_characters.json'),
        'comicvine':     load_json('data/comicvine_characters.json'),
    }

    for name, d in sources.items():
        if d is None and name != 'comicvine':
            print(f"  WARNING: {name} silo not found — skipping")

    active = {k: v for k, v in sources.items() if v}
    nodes  = merge_nodes(active)

    # 2022 character edges are stored PMI*1000 (spatialized); comicvine edges are raw
    edges = merge_edges(
        active,
        spatialized_sources={'marvel_2022'}
    )

    return nodes, edges


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Merge data silos into a single graph')
    parser.add_argument('--creators',   action='store_true')
    parser.add_argument('--characters', action='store_true')
    parser.add_argument('--output', required=True, help='Output JSON path')
    args = parser.parse_args()

    if args.creators:
        nodes, edges = merge_creators()
    elif args.characters:
        nodes, edges = merge_characters()
    else:
        print("Specify --creators or --characters")
        return

    output = {
        "options": {"type": "undirected", "multi": False, "allowSelfLoops": False},
        "attributes": {},
        "nodes": nodes,
        "edges": edges
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print("\n" + "=" * 60)
    print("MERGE COMPLETE")
    print("=" * 60)
    print(f"  Nodes : {len(nodes)}")
    print(f"  Edges : {len(edges)}")
    print(f"  Output: {out_path}")

    source_counts = defaultdict(int)
    for node in nodes:
        for src in node['attributes'].get('sources', []):
            source_counts[src] += 1
    print("\n  Source distribution:")
    for src, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {src}: {count}")


if __name__ == "__main__":
    main()
