# Marvel Data Import Scripts

This document explains how to import Marvel data from alternative APIs since the official Marvel API is currently down.

## Available Import Scripts

### 1. SuperHero API (Characters Only)
**No API key required!**

```bash
python bin/import_superhero_characters.py
```

**What it provides:**
- ✅ 269 Marvel characters
- ✅ Character metadata (name, powers, bio)
- ❌ No comics data
- ❌ No co-occurrence data

**Output:** `data/superhero_characters.json`

**Use case:** Character metadata supplementation only

---

### 2. EmreParker API (Creators + Comics)
**No API key required! Data up to 2024!**

```bash
# Import creators network (takes ~30 minutes with rate limiting)
python bin/import_emreparker_data.py

# Or just creators:
python bin/import_emreparker_data.py --creators-only

# Or just comics:
python bin/import_emreparker_data.py --comics-only

# Test with fewer creators:
python bin/import_emreparker_data.py --max-creators 50
```

**What it provides:**
- ✅ 4,341 creators
- ✅ ~37,500 issues/comics (up to 2024!)
- ✅ Creator co-occurrence network
- ✅ Comics with creator metadata
- ❌ No character data

**Output:**
- `data/emreparker_creators.json` (creators network)
- `data/emreparker_comics.csv.gz` (comics database)

**Rate limit:** 60 requests/minute (built-in 1 second delay)

**Use case:** Update creators network to 2024

---

### 3. Comic Vine API (Full Data)
**Requires free API key!**

**Get your API key:**
1. Go to https://comicvine.gamespot.com/api/
2. Login or create account
3. Copy your API key from the account page

```bash
# Import characters (WARNING: Very slow with rate limits)
python bin/import_comicvine_data.py --api-key YOUR_API_KEY --characters-only

# Test with limited characters:
python bin/import_comicvine_data.py --api-key YOUR_API_KEY --max-chars 50

# Import creators:
python bin/import_comicvine_data.py --api-key YOUR_API_KEY --creators-only
```

**What it provides:**
- ✅ Characters
- ✅ Creators
- ✅ Issues/Comics
- ✅ Character co-occurrence
- ✅ Creator co-occurrence

**Output:**
- `data/comicvine_characters.json` (characters network)
- `data/comicvine_creators.json` (creators network)

**Rate limit:** 200 requests/hour (18 seconds between requests)

**Warning:** Very slow! Fetching all characters with their issues could take days.

**Use case:** Full data update when official API is down

---

## Schema Comparison

### Current MarvelGraph Format

**Character Node:**
```json
{
  "key": "1011334",
  "attributes": {
    "label": "3-D Man",
    "image": "./images/characters/1011334.jpg",
    "image_url": "http://...",
    "url": "http://marvel.com/...",
    "stories": 12,
    "description": "...",
    "community": 0
  }
}
```

**Comics CSV:**
```
id,title,date,description,characters,writers,artists,image_url,url
```

### Import Script Mappings

| Field | SuperHero | EmreParker | Comic Vine |
|-------|-----------|------------|------------|
| Characters | ✅ (basic) | ❌ | ✅ (full) |
| Creators | ❌ | ✅ (full) | ✅ (full) |
| Comics | ❌ | ✅ (2024!) | ✅ |
| Co-occurrence | ❌ | ✅ (creators) | ✅ (both) |
| API Key | No | No | Yes |
| Rate Limit | None | 60/min | 200/hour |

---

## Recommended Workflow

### Quick Update (Creators to 2024)
```bash
# 1. Import creators from EmreParker (no key needed, up to 2024)
python bin/import_emreparker_data.py --creators-only

# 2. Keep your existing 2022 character data
# You now have:
#   - Characters: 2022 data
#   - Creators: 2024 data
```

### Full Update (When You Have Time)
```bash
# 1. Get Comic Vine API key
# 2. Import characters (slow!)
python bin/import_comicvine_data.py --api-key YOUR_KEY --characters-only --max-chars 100

# 3. Import creators from EmreParker (faster, more recent)
python bin/import_emreparker_data.py --creators-only
```

### Test First
```bash
# Test each script with limited data
python bin/import_superhero_characters.py
python bin/import_emreparker_data.py --max-creators 10
python bin/import_comicvine_data.py --api-key YOUR_KEY --max-chars 10
```

---

## Next Steps After Import

After importing data, you need to:

1. **Spatialize the network** (position nodes)
   ```bash
   node spatialize-network.js data/emreparker_creators.json
   ```

2. **Compress for web**
   ```bash
   python -c "import zlib, json; data=json.load(open('data/emreparker_creators.json')); open('data/emreparker_creators.json.gz','wb').write(zlib.compress(json.dumps(data).encode()))"
   ```

3. **Update the web app** to load new data files

---

## Troubleshooting

**"Rate limit exceeded"**
- EmreParker: Wait 1 minute
- Comic Vine: Wait 1 hour

**"API key invalid"**
- Check you copied the full key
- Make sure you're logged into Comic Vine

**"No module named 'requests'"**
```bash
pip install requests
```

**Script is too slow**
- Use `--max-creators` or `--max-chars` to test with less data
- For Comic Vine, consider running overnight

---

## When Official Marvel API Returns

Check if it's back online:
```bash
python check_api.py
```

If online, use the original script:
```bash
python bin/download_data.py
```
