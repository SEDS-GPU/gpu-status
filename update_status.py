#!/usr/bin/env python3
"""
Reads users.csv and scrapes the cluster status page.
Writes status.json with only counts — no usernames ever leave this script.
"""

import csv
import json
import re
import urllib.request
from datetime import datetime, timezone

STATUS_URL   = 'https://ccu-k8s.inf.uni-konstanz.de/status.html'
JUPYTER_NODE = 'kiaransalee'

STUDENT_LIMIT    = 2
RESEARCHER_LIMIT = 4


def load_users(path='users.csv'):
    users = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for row in reader:
            username = row.get('github account', '').strip().lower()
            role     = row.get('category', '').strip().lower()
            if username and role:
                users[username] = role
    return users


def fetch_status():
    req = urllib.request.Request(STATUS_URL, headers={'User-Agent': 'gpu-status-bot/1.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read().decode('utf-8', errors='replace')


def parse_status(html, users):
    # Strip HTML tags but preserve newlines from <br> and block elements
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<(?:tr|p|div|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;',  '&', text)

    # ── Pod counting ──────────────────────────────────────────────────────────
    # Find the nvidia.com/gpu section, then within it find the slice of text
    # between "kiaransalee" and the next node name (or next resource type).
    # Node names in this cluster: asmodeus, belial, demogorgon, fierna,
    # kiaransalee, tiamat, vecna, zariel — all end before the next │ ├ └ tree char
    # at the same level.
    #
    # Simpler approach: find "kiaransalee" inside the gpu section, then
    # extract text until the next occurrence of another node or "nvidia.com".

    student_active    = 0
    researcher_active = 0

    # Isolate the nvidia.com/gpu section (everything before the first nvidia.com/mig)
    gpu_match = re.search(r'nvidia\.com/gpu(.*?)nvidia\.com/mig', text, re.DOTALL)
    if gpu_match:
        gpu_section = gpu_match.group(1)

        # Within the gpu section, find the kiaransalee block:
        # text from "kiaransalee" up to the next node-level entry
        # Node-level entries are preceded by tree chars (├─ or └─) with similar indentation
        node_match = re.search(
            r'kiaransalee(.*?)(?=├─|└─|nvidia\.com|$)',
            gpu_section, re.DOTALL
        )
        if node_match:
            node_block = node_match.group(1)
            print("DEBUG node_block:", repr(node_block[:300]))

            kiaransalee_pods = set(
                m.group(1).lower()
                for m in re.finditer(r'jupyter-([\w-]+)', node_block)
            )
            print("DEBUG kiaransalee_pods:", kiaransalee_pods)

            for pod in kiaransalee_pods:
                role = users.get(pod)
                if role == 'student': student_active    += 1
                else:                 researcher_active += 1

    # ── MIG free slice counting (mig-1g.10gb only) ───────────────────────────
    free_mig  = 0
    mig_match = re.search(r'nvidia\.com/mig-1g\.10gb(.*?)(?=nvidia\.com/mig|$)', text, re.DOTALL)
    if mig_match:
        mig_section = mig_match.group(1)
        node_line   = re.search(r'kiaransalee[^\n]*', mig_section)
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
