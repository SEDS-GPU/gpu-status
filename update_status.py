#!/usr/bin/env python3
"""
Reads users.csv and scrapes the cluster status page.
Writes status.json with only counts — no usernames ever leave this script.

Counting rules:
- nvidia.com/gpu -> kiaransalee: all pods except system ones count.
  Known role in CSV = that role. Unknown = researcher.
- nvidia.com/mig-* -> kiaransalee: all jupyter-* pods count.
  Known role in CSV = that role. Unknown = student.
"""

import csv
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

STATUS_URL   = 'https://ccu-k8s.inf.uni-konstanz.de/status.html'
JUPYTER_NODE = 'kiaransalee'

STUDENT_LIMIT    = 2
RESEARCHER_LIMIT = 4

# Pod name prefixes that are system/infrastructure — never count these
SYSTEM_PREFIXES = ('kube-', 'dnsutils-')


def load_users(path='users.csv'):
    users = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for row in reader:
            username = row.get('github account', '').strip().lower()
            role     = (row.get('category') or '').strip().lower()
            if username and role:
                users[username] = role
    return users


def fetch_status(retries=3, delay=10):
    req = urllib.request.Request(STATUS_URL, headers={'User-Agent': 'gpu-status-bot/1.0'})
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.read().decode('utf-8', errors='replace')
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Failed to fetch status page after {retries} attempts")


def extract_node_block(text, section_pattern, node):
    """
    Find a section matching section_pattern, then extract the block of text
    between node and the next sibling node or section boundary.
    Returns the block text or None.
    """
    section_match = re.search(section_pattern, text, re.DOTALL)
    if not section_match:
        return None
    section_text = section_match.group(1)
    node_match = re.search(
        re.escape(node) + r'(.*?)(?=&#9500;&#9472;|&#9492;&#9472;|nvidia\.com|$)',
        section_text, re.DOTALL
    )
    if not node_match:
        return None
    return node_match.group(1)


def get_pods_from_block(block):
    """Extract all pod names from a node block, excluding system pods."""
    pods = set()
    for m in re.finditer(r'([\w][\w-]+)', block):
        name = m.group(1).lower()
        # Skip numbers, short tokens, and system pods
        if len(name) < 3:
            continue
        if any(name.startswith(p) for p in SYSTEM_PREFIXES):
            continue
        # Skip tokens that look like numbers or percentages
        if re.match(r'^[\d.]+$', name):
            continue
        pods.add(name)
    return pods


def get_jupyter_pods_from_block(block):
    """Extract only jupyter-USERNAME pods from a node block."""
    return set(
        m.group(1).lower()
        for m in re.finditer(r'jupyter-([\w-]+)', block)
    )


def parse_status(html, users):
    # Strip HTML tags but preserve structure
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<(?:tr|p|div|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&amp;', '&', text)
    # Keep HTML entities for tree chars as-is (&#9474; &#9492; &#9472; &#9500;)
    # so we can use them as delimiters

    student_active    = 0
    researcher_active = 0

    # ── GPU section: count all non-system pods on kiaransalee ─────────────────
    gpu_block = extract_node_block(
        text,
        r'nvidia\.com/gpu(.*?)nvidia\.com/mig',
        JUPYTER_NODE
    )
    if gpu_block:
        # Get all pod-like tokens, excluding system pods
        for m in re.finditer(r'jupyter-([\w-]+)|([\w][\w-]{2,})', gpu_block):
            if m.group(1):
                # jupyter-USERNAME pod
                name = m.group(1).lower()
                role = users.get(name)
                if role == 'student': student_active    += 1
                else:                 researcher_active += 1
            else:
                name = m.group(2).lower()
                # Skip system pods, numbers, tree artifacts, and jupyter- (already handled)
                if any(name.startswith(p) for p in SYSTEM_PREFIXES):
                    continue
                if re.match(r'^[\d.]+$', name):
                    continue
                if name in ('kiaransalee', 'nvidia', 'com', 'gpu', 'mig'):
                    continue
                # This is a manually created pod — count as researcher unless in CSV
                role = users.get(name)
                if role == 'student': student_active    += 1
                else:                 researcher_active += 1

    # ── MIG sections: count jupyter-* pods on kiaransalee as students ─────────
    # Find ALL mig sections and collect jupyter pods across all of them
    mig_jupyter_pods = set()
    for mig_match in re.finditer(r'nvidia\.com/mig-[\w."]+(.*?)(?=nvidia\.com/|$)', text, re.DOTALL):
        mig_section = mig_match.group(1)
        node_match  = re.search(
            re.escape(JUPYTER_NODE) + r'(.*?)(?=&#9500;&#9472;|&#9492;&#9472;|nvidia\.com|$)',
            mig_section, re.DOTALL
        )
        if node_match:
            mig_jupyter_pods |= get_jupyter_pods_from_block(node_match.group(1))

    for pod in mig_jupyter_pods:
        role = users.get(pod)
        if role == 'researcher': researcher_active += 1
        else:                    student_active    += 1  # known student OR unknown = student

    # ── MIG free slice counting (mig-1g.10gb only) ───────────────────────────
    free_mig  = 0
    mig_match = re.search(r'nvidia\.com/mig-1g\.10gb(.*?)(?=nvidia\.com/mig|$)', text, re.DOTALL)
    if mig_match:
        node_line = re.search(r'kiaransalee[^\n]*', mig_match.group(1))
        if node_line:
            nums = re.findall(r'[\d.]+', node_line.group(0))
            if nums:
                try:
                    free_mig = int(float(nums[-1]))
                except ValueError:
                    pass

    # ── Timestamp ─────────────────────────────────────────────────────────────
    ts_match  = re.search(r'Cluster status\s+([\w,: +]+\d{4})', text)
    timestamp = ts_match.group(1).strip() if ts_match else None

    return dict(
        studentActive=student_active,
        researcherActive=researcher_active,
        freeMIG=free_mig,
        studentLimit=STUDENT_LIMIT,
        researcherLimit=RESEARCHER_LIMIT,
        timestamp=timestamp,
        updatedAt=datetime.now(timezone.utc).isoformat(),
    )


if __name__ == '__main__':
    users  = load_users('users.csv')
    html   = fetch_status()
    result = parse_status(html, users)

    with open('status.json', 'w') as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))
