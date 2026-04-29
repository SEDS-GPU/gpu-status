#!/usr/bin/env python3
"""
Reads users.csv and scrapes the cluster status page.
Writes status.json with only counts — no usernames ever leave this script.

Counting rules:
- nvidia.com/gpu -> kiaransalee: all pods count except system ones (kube-*, dnsutils-*).
  jupyter-USERNAME: look up CSV, unknown = researcher.
  manually named pods: look up CSV, unknown = researcher.
- nvidia.com/mig-* -> kiaransalee: jupyter-* pods only.
  look up CSV, unknown = student.
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


def get_kiaransalee_pods(lines, start_idx, end_idx):
    """
    Given lines between start_idx and end_idx, find the kiaransalee node
    and return all pod names under it.

    Pod lines are distinguished from node lines by containing &#9474; (│),
    which is the tree character used for child entries.
    """
    # Find kiaransalee line
    node_idx = None
    for i in range(start_idx, end_idx):
        if JUPYTER_NODE in lines[i]:
            node_idx = i
            break
    if node_idx is None:
        return set(), set()  # (jupyter_pods, manual_pods)

    jupyter_pods = set()
    manual_pods  = set()

    for i in range(node_idx + 1, end_idx):
        line = lines[i]
        # Pod lines contain &#9474; (│) — child indentation marker
        # Node-sibling lines do not
        if '&#9474;' not in line:
            break  # back to node level, kiaransalee block ended

        # Check for jupyter-USERNAME pod
        m = re.search(r'jupyter-([\w-]+)', line)
        if m:
            jupyter_pods.add(m.group(1).lower())
            continue

        # Manual pod: strip all HTML entities and tree chars, get first token
        clean = re.sub(r'&#\d+;', ' ', line).strip()
        tokens = clean.split()
        if not tokens:
            continue
        name = tokens[0].lower()

        # Skip system pods, numeric tokens, and very short tokens
        if any(name.startswith(p) for p in SYSTEM_PREFIXES):
            continue
        if re.match(r'^[\d.]+', name):
            continue
        if len(name) < 3:
            continue

        manual_pods.add(name)

    return jupyter_pods, manual_pods


def parse_status(html, users):
    # Strip HTML tags but preserve newlines and keep HTML entities intact
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<(?:tr|p|div|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&amp;', '&', text)

    lines = text.splitlines()

    # Find section boundaries
    gpu_start = gpu_end = None
    mig_starts = []

    for i, line in enumerate(lines):
        if 'nvidia.com/gpu' in line and 'mig' not in line.lower() and gpu_start is None:
            gpu_start = i
        elif gpu_start is not None and gpu_end is None and 'nvidia.com/mig' in line:
            gpu_end = i
            mig_starts.append(i)
        elif 'nvidia.com/mig' in line:
            mig_starts.append(i)

    student_active    = 0
    researcher_active = 0

    # ── GPU section ───────────────────────────────────────────────────────────
    if gpu_start is not None and gpu_end is not None:
        jupyter_pods, manual_pods = get_kiaransalee_pods(lines, gpu_start, gpu_end)
        print("DEBUG GPU jupyter_pods:", jupyter_pods)
        print("DEBUG GPU manual_pods:", manual_pods)

        for pod in jupyter_pods:
            role = users.get(pod)
            if role == 'student': student_active    += 1
            else:                 researcher_active += 1  # unknown = researcher

        for pod in manual_pods:
            role = users.get(pod)
            if role == 'student': student_active    += 1
            else:                 researcher_active += 1  # unknown = researcher

    # ── MIG sections ─────────────────────────────────────────────────────────
    mig_jupyter_pods = set()
    for idx, mig_start in enumerate(mig_starts):
        mig_end = mig_starts[idx + 1] if idx + 1 < len(mig_starts) else len(lines)
        j_pods, _ = get_kiaransalee_pods(lines, mig_start, mig_end)
        mig_jupyter_pods |= j_pods

    print("DEBUG MIG jupyter_pods:", mig_jupyter_pods)
    for pod in mig_jupyter_pods:
        role = users.get(pod)
        if role == 'researcher': researcher_active += 1
        else:                    student_active    += 1  # unknown = student

    # ── MIG free slice counting (mig-1g.10gb only) ───────────────────────────
    free_mig = 0
    for i, line in enumerate(lines):
        if 'nvidia.com/mig-1g' in line:
            # Look for kiaransalee in the next few lines
            for j in range(i + 1, min(i + 10, len(lines))):
                if JUPYTER_NODE in lines[j]:
                    nums = re.findall(r'[\d.]+', lines[j])
                    if nums:
                        try:
                            free_mig = int(float(nums[-1]))
                        except ValueError:
                            pass
                    break
            break

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
