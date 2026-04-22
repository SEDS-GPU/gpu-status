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
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;',  '&', text)

    # ── Pod counting ──────────────────────────────────────────────────────────
    # Search the ENTIRE page for jupyter-USERNAME pods — no need to limit to
    # a node block since our users dict only contains usernames we care about.
    active_pods = [m.group(1).lower()
                   for m in re.finditer(r'jupyter-([\w-]+)', text)]

    student_active    = 0
    researcher_active = 0
    for pod in active_pods:
        role = users.get(pod)
        if   role == 'student':    student_active    += 1
        elif role == 'researcher': researcher_active += 1

    # ── MIG free slice counting ───────────────────────────────────────────────
    # The status page lists each MIG type then an indented line per node, e.g.:
    #   nvidia.com/mig-1g.10gb   (0%) 0.0   (0%) 0.0   7.0   7.0
    #   └─ kiaransalee           (0%) 0.0   (0%) 0.0   7.0   7.0
    # The last number on the kiaransalee line is the free count.
    free_mig = 0
    lines = text.splitlines()
    for line in lines:
        if JUPYTER_NODE in line and 'mig' not in line.lower():
            continue  # skip non-MIG kiaransalee lines
        if JUPYTER_NODE in line:
            nums = re.findall(r'[\d.]+', line)
            if nums:
                try:
                    free_mig += float(nums[-1])
                except ValueError:
                    pass

    # Fallback: scan for kiaransalee lines that follow a nvidia.com/mig header
    if free_mig == 0:
        in_mig_section = False
        for line in lines:
            if re.search(r'nvidia\.com/mig-', line):
                in_mig_section = True
            elif in_mig_section and JUPYTER_NODE in line:
                nums = re.findall(r'[\d.]+', line)
                if nums:
                    try:
                        free_mig += float(nums[-1])
                    except ValueError:
                        pass
                in_mig_section = False
            elif in_mig_section and line.strip() and JUPYTER_NODE not in line and 'nvidia' not in line.lower():
                in_mig_section = False

    # ── Timestamp ─────────────────────────────────────────────────────────────
    ts_match  = re.search(r'Cluster status\s+([\w,: +]+\d{4})', text)
    timestamp = ts_match.group(1).strip() if ts_match else None

    return dict(
        studentActive=student_active,
        researcherActive=researcher_active,
        freeMIG=int(free_mig),
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
