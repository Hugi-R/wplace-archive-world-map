#!/usr/bin/env python3
"""
Download assets from bczhc/wplace-diffs GitHub releases.
Only downloads releases newer than the latest already-downloaded asset.
"""

import os
import re
import requests

ASSET_FOLDER = os.environ.get("BCZHC_WPLACE_DIFFS_FOLDER")
if not ASSET_FOLDER:
    raise ValueError("Please set BCZHC_WPLACE_DIFFS_FOLDER environment variable to the target folder for downloaded assets.")
REPO = "bczhc/wplace-diffs"
API_BASE = f"https://api.github.com/repos/{REPO}/releases"
ASSET_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d+Z)\.diff\.zst$")

# Optional: set a GitHub token to avoid rate limiting
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}


def get_latest_local_date() -> str | None:
    """Return the latest release name already present in ASSET_FOLDER, or None."""
    dates = []
    for fname in os.listdir(ASSET_FOLDER):
        m = ASSET_PATTERN.match(fname)
        if m:
            dates.append(m.group(1))
    return max(dates) if dates else None


def fetch_all_releases() -> list[dict]:
    """Fetch all releases via paginated GitHub API."""
    releases = []
    url = f"{API_BASE}?per_page=100&page=1"
    while url:
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        releases.extend(resp.json())
        # Follow pagination via Link header
        link = resp.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                break
        url = next_url
    return releases


def main():
    os.makedirs(ASSET_FOLDER, exist_ok=True)

    latest_local = get_latest_local_date()
    print(f"Latest local asset: {latest_local or '(none)'}")

    print("Fetching release list from GitHub...")
    releases = fetch_all_releases()

    # Filter to releases with the expected naming pattern
    valid = []
    for r in releases:
        name = r.get("tag_name") or r.get("name", "")
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d+Z$", name):
            valid.append((name, r))

    # Only keep releases newer than the latest local asset
    if latest_local:
        to_download = [(name, r) for name, r in valid if name > latest_local]
    else:
        to_download = valid

    to_download.sort(key=lambda x: x[0])

    if not to_download:
        print("Nothing new to download.")
        return

    print(f"{len(to_download)} new release(s) to download.")

    for name, release in to_download:
        asset_name = f"{name}.diff.zst"
        # Find the matching asset in the release
        asset_url = None
        for asset in release.get("assets", []):
            if asset["name"] == asset_name:
                asset_url = asset["browser_download_url"]
                break

        if not asset_url:
            print(f"  [SKIP] {name}: asset '{asset_name}' not found in release.")
            continue

        dest = os.path.join(ASSET_FOLDER, asset_name)
        if os.path.exists(dest):
            print(f"  [SKIP] {asset_name}: already exists.")
            continue

        print(f"  [DOWN] {asset_name} ...", end=" ", flush=True)
        with requests.get(asset_url, headers=HEADERS, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            downloaded = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
            size_kb = downloaded / (1024*1024)
            print(f"done ({size_kb:.1f} MB)")

    print("All done.")


if __name__ == "__main__":
    main()