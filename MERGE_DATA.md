# Merging Data from Multiple Sources

## Overview

The import scripts now **automatically tag their source** and include **deduplication indexes**:

| Script | Source Tag | ID Type | Dedup Index |
|--------|-----------|---------|-------------|
| `import_superhero_characters.py` | `superhero_api` | Custom | `source_id` |
| `import_emreparker_data.py` | `emreparker_2024` | **Marvel ID** | `marvel_id` |
| `import_comicvine_data.py` | `comicvine` | Comic Vine | `comicvine_id` |
| Original Marvel data | `marvel_official_2022` | **Marvel ID** | (key) |

## Key Insight: EmreParker Uses Marvel IDs! ✅

**EmreParker creator IDs are the same as Marvel's official IDs** because it was built from cached Marvel API data. This means:

- ✅ Direct merge possible with 2022 Marvel data
- ✅ No name matching needed
- ✅ Perfect deduplication
- ✅ Can safely combine 2022 + 2024 data

## Merge Commands

### Merge Creators (2022 + 2024 Data)

```bash
cd h:/GitHub/MarvelGraph

# Wait for the background import to finish first!
# Check with: tail -10 C:/Users/Stuart/AppData/Local/Temp/claude/h--GitHub-MarvelGraph/tasks/b3cabc9.output

# Then merge:
python bin/merge_data_sources.py --creators --output data/Marvel_creators_merged.json
```

**Result:**
- Combines 2022 Marvel data + 2024 EmreParker data
- Deduplicates by Marvel ID
- Sums story counts from both sources
- Tags each node with its sources

### Merge Characters (Multiple Sources)

```bash
python bin/merge_data_sources.py --characters --output data/Marvel_characters_merged.json
```

**Result:**
- Combines Marvel 2022 + SuperHero API + Comic Vine (if available)
- Uses Marvel IDs where available
- Falls back to name matching for SuperHero data
- Tags each node with its sources

## What the Merge Does

### 1. ID-Based Deduplication (Preferred)
```
Marvel 2022:  Creator ID 11743 = "Jonathan Hickman" (100 stories)
EmreParker 2024: Creator ID 11743 = "Jonathan Hickman" (395 stories)

Merged: Creator ID 11743 = "Jonathan Hickman" (495 stories total)
        sources: ["marvel_official_2022", "emreparker_2024"]
```

### 2. Name-Based Fallback (For SuperHero)
```
Marvel:     "Spider-Man (Peter Parker)"
SuperHero:  "Spider-Man"

Merged: Match by name normalization
```

### 3. Attribute Merging
- **Stories/counts**: Summed across sources
- **URLs/images**: Prefers non-empty values
- **Descriptions**: Prefers longest/most detailed

### 4. Source Tracking
Every node gets a `sources` array:
```json
{
  "key": "11743",
  "attributes": {
    "label": "Jonathan Hickman",
    "stories": 495,
    "sources": ["marvel_official_2022", "emreparker_2024"],
    "marvel_id": "11743"
  }
}
```

## Workflow Examples

### Update Creators to 2024

```bash
# 1. You already have 2022 data: data/Marvel_creators_by_stories_full.json.gz
# 2. Background import is running to get 2024 data
# 3. When it finishes, merge:

python bin/merge_data_sources.py --creators --output data/Marvel_creators_2024.json

# Result: Combined dataset with 2022-2024 data
```

### Enrich Character Metadata

```bash
# Add extra character info from SuperHero API
python bin/merge_data_sources.py --characters --output data/Marvel_characters_enriched.json

# Result: Your 2022 character network + bonus metadata from SuperHero
```

## Checking Merge Quality

After merging, inspect the results:

```bash
# Check source distribution
python -c "
import json
from collections import Counter
data = json.load(open('data/Marvel_creators_2024.json'))
sources = [s for n in data['nodes'] for s in n['attributes'].get('sources', [])]
print('Source distribution:')
for source, count in Counter(sources).most_common():
    print(f'  {source}: {count} nodes')
"
```

Example output:
```
Source distribution:
  marvel_official_2022: 2,156 nodes
  emreparker_2024: 4,341 nodes
  (merged): 1,523 nodes in both
```

## When to Merge vs. Keep Separate

### ✅ Good to Merge:
- **Creators**: 2022 + 2024 data (same ID system)
- **Characters**: Enriching with extra metadata
- **Cross-validation**: Checking data consistency

### ⚠️ Keep Separate:
- **Different time periods**: If you want to compare 2022 vs 2024
- **Source analysis**: Studying differences between APIs
- **Testing**: Before committing to merged data

## Troubleshooting

**"KeyError: 'nodes'"**
- One of the source files doesn't exist
- Check which files you have with `ls data/*.json*`

**"Too many duplicate warnings"**
- Normal! Name matching has false positives
- Check the output `sources` array to verify

**"Stories count seems too high"**
- Counts are summed across sources
- Check the `sources` array - might be triple-counted if in 3 datasets

## Advanced: Custom Merge

Edit `bin/merge_data_sources.py` to:
- Change merge priority (which source wins for conflicts)
- Add custom matching rules
- Filter out certain sources
- Weight story counts differently

---

**After merging, remember to:**
1. Spatialize the network: `node spatialize-network.js data/Marvel_creators_2024.json`
2. Compress for web: `python -c "import zlib, json; ..."`
3. Update your web app to load the merged file
