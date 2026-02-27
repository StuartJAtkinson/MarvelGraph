#!/usr/bin/env python3
"""Quick script to check if Marvel API is back online"""

import requests
import sys

try:
    # Test the API without authentication
    response = requests.get(
        "https://gateway.marvel.com/v1/public/characters",
        params={"limit": 1},
        timeout=10
    )

    if response.status_code == 200:
        print("[OK] Marvel API is ONLINE!")
        print("You can now get API keys from: https://developer.marvel.com/account")
        sys.exit(0)
    elif response.status_code == 409:
        print("[OK] Marvel API is responding (but needs authentication)")
        print("Get API keys from: https://developer.marvel.com/account")
        sys.exit(0)
    else:
        print(f"[ERROR] Marvel API returned status: {response.status_code}")
        print("Still having issues. Try again later.")
        sys.exit(1)

except Exception as e:
    print("[ERROR] Marvel API is DOWN or unreachable")
    print(f"Error: {e}")
    sys.exit(1)
