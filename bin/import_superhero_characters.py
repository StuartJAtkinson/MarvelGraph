#!/usr/bin/env python3
"""
Import Marvel characters from the free akabab superhero API
No API key required!
"""

import json
import requests
from pathlib import Path

API_BASE = "https://cdn.jsdelivr.net/gh/akabab/superhero-api@0.3.0/api"

def download_all_heroes():
    """Download all superheroes from the API"""
    print("Downloading all heroes from akabab superhero API...")
    response = requests.get(f"{API_BASE}/all.json")
    response.raise_for_status()
    return response.json()

def filter_marvel_characters(heroes):
    """Filter only Marvel Comics characters"""
    marvel = [h for h in heroes if h.get('biography', {}).get('publisher') == 'Marvel Comics']
    print(f"Found {len(marvel)} Marvel characters out of {len(heroes)} total")
    return marvel

def convert_to_graph_format(characters):
    """Convert superhero API format to MarvelGraph format"""
    nodes = []

    for char in characters:
        node = {
            "key": str(char['id']),
            "attributes": {
                "label": char['name'],
                "image": "",  # We don't download images, just reference
                "image_url": char['images']['lg'] if 'images' in char else "",
                "url": f"https://superheroapi.com/character/{char['slug']}",
                "stories": 1,  # We don't have story count from this API
                "description": char.get('biography', {}).get('fullName', ''),
                "x": 0,  # Will be spatialized later
                "y": 0,
                "size": None,
                "community": 0,  # Will be computed later
                "source": "superhero_api",
                "source_id": char['id']
            }
        }
        nodes.append(node)

    return {
        "options": {
            "type": "undirected",
            "multi": False,
            "allowSelfLoops": False
        },
        "attributes": {},
        "nodes": nodes,
        "edges": []  # No co-occurrence data from this API
    }

def save_graph(graph_data, output_path):
    """Save graph data as JSON"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, indent=2)

    print(f"Saved {len(graph_data['nodes'])} characters to {output_path}")

def main():
    # Download data
    heroes = download_all_heroes()

    # Filter Marvel characters
    marvel_chars = filter_marvel_characters(heroes)

    # Convert to graph format
    graph = convert_to_graph_format(marvel_chars)

    # Save output
    output_file = "data/superhero_characters.json"
    save_graph(graph, output_file)

    print("\n[OK] Import complete!")
    print(f"  Characters: {len(graph['nodes'])}")
    print(f"  Output: {output_file}")
    print("\nNote: This data has no story co-occurrence info.")
    print("      Use it to supplement character metadata only.")

if __name__ == "__main__":
    main()
