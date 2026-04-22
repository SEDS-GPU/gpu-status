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
    # Strip HTML tags but preserve newlines
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;',  '&', text)

    lines = text.splitlines()

    # ── Pod counting ──────────────────────────────────────────────────────────
    # Strategy:
    # 1. Find the line containing "nvidia.com/gpu"
    # 2. Within that section, find the line containing JUPYTER_NODE
    # 3. Collect all jupyter-* pods indented under that node
    #    until we hit a line at the same or lower indentation level

    student_active    = 0
    researcher_active = 0

    # Find nvidia.com/gpu section start
    gpu_section_start = None
    for i, line in enumerate(lines):
        if 'nvidia.com/gpu' in line and 'mig' not in line.lower():
            gpu_section_start = i
            break

    if gpu_section_start is not None:
        # Find kiaransalee within the gpu section
        node_line_idx = None
        node_indent   = None
        for i in range(gpu_section_start + 1, len(lines)):
            line = lines[i]
            # Stop if we hit the next resource type (e.g. nvidia.com/mig or memory section)
            if re.match(r'\s{0,4}\S', line) and i > gpu_section_start + 1:
                if 'nvidia.com' in line or ('kiaransalee' not in line and node_line_idx is not None):
                    if node_line_idx is not None:
                        break
            if JUPYTER_NODE in line:
                node_line_idx = i
                # Measure indentation of the node line
                node_indent = len(line) - len(line.lstrip())
                break

        if node_line_idx is not None:
            # Collect jupyter-* pods on lines MORE indented than the node line
            kiaransalee_pods = set()
            for i in range(node_line_idx + 1, len(lines)):
                line = lines[i]
                if not line.strip():
                    continue
                line_indent = len(line) - len(line.lstrip())
                # Stop when we're back to node-level indentation or less
                if line_indent <= node_indent:
                    break
                m = re.search(r'jupyter-([\w-]+)', line)
                if m:
                    kiaransalee_pods.add(m.group(1).lower())

            for pod in kiaransalee_pods:
                role = users.get(pod)
                if role == 'student': student_active    += 1
                else:                 researcher_active += 1  # known researcher OR unknown = researcher

    # ── MIG free slice counting ───────────────────────────────────────────────
    # Only count mig-1g.10gb slices (the small student-usable ones) on kiaransalee.
    # The last number on the kiaransalee line is the free count.
    free_mig     = 0
    in_mig_1g    = False
    for line in lines:
        if 'nvidia.com/mig-1g' in line:
            in_mig_1g = True
            continue
        if in_mig_1g:
            if JUPYTER_NODE in line:
                nums = re.findall(r'[\d.]+', line)
                if nums:
                    try:
                        free_mig = int(float(nums[-1]))
                    except ValueError:
                        pass
            # Stop at next nvidia.com/ line
            if 'nvidia.com' in line:
                in_mig_1g = False

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
