#!/usr/bin/env python3
"""
Reads users.csv and scrapes the cluster status page.
Writes status.json with only counts — no usernames ever leave this script.

Counting rules:
- nvidia.com/gpu -> kiaransalee: all pods except system ones count.
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
MIG_LIMIT        = 4

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
    and return all pod names under it as (jupyter_pods, manual_pods).
    Pod lines are distinguished by containing &#9474; (│).
    """
    node_idx = None
    for i in range(start_idx, end_idx):
        if JUPYTER_NODE in lines[i]:
            node_idx = i
            break
    if node_idx is None:
        return set(), set()

    jupyter_pods = set()
    manual_pods  = set()

    for i in range(node_idx + 1, end_idx):
        line = lines[i]
        if '&#9474;' not in line and '&#9492;' not in line:
            break

        m = re.search(r'jupyter-([\w-]+)', line)
        if m:
            jupyter_pods.add(m.group(1).lower())
            continue

        clean  = re.sub(r'&#\d+;', ' ', line).strip()
        tokens = clean.split()
        if not tokens:
            continue
        name = tokens[0].lower()

        if any(name.startswith(p) for p in SYSTEM_PREFIXES):
            continue
        if re.match(r'^[\d.]+', name):
            continue
        if len(name) < 3:
            continue

        manual_pods.add(name)

    return jupyter_pods, manual_pods


def parse_status(html, users):
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

    # Four counters
    student_gpu    = 0  # students on whole GPU slots
    researcher_gpu = 0  # researchers on whole GPU slots
    student_mig    = 0  # students on MIG slices
    researcher_mig = 0  # researchers on MIG slices

    # ── GPU section ───────────────────────────────────────────────────────────
    if gpu_start is not None and gpu_end is not None:
        jupyter_pods, manual_pods = get_kiaransalee_pods(lines, gpu_start, gpu_end)

        for pod in jupyter_pods:
            role = users.get(pod)
            if role == 'student': student_gpu    += 1
            else:                 researcher_gpu += 1

        for pod in manual_pods:
            role = users.get(pod)
            if role == 'student': student_gpu    += 1
            else:                 researcher_gpu += 1

    # ── MIG sections ─────────────────────────────────────────────────────────
    mig_jupyter_pods = set()
    for idx, mig_start in enumerate(mig_starts):
        mig_end = mig_starts[idx + 1] if idx + 1 < len(mig_starts) else len(lines)
        j_pods, _ = get_kiaransalee_pods(lines, mig_start, mig_end)
        mig_jupyter_pods |= j_pods

    for pod in mig_jupyter_pods:
        role = users.get(pod)
        if role == 'researcher': researcher_mig += 1
        else:                    student_mig    += 1

    # ── MIG free slice counting (mig-3g.40gb and mig-4g.40gb) ────────────────
    free_mig = 0
    for i, line in enumerate(lines):
        if 'nvidia.com/mig-3g' in line or 'nvidia.com/mig-4g' in line:
            for j in range(i + 1, min(i + 10, len(lines))):
                if JUPYTER_NODE in lines[j]:
                    nums = re.findall(r'[\d.]+', lines[j])
                    if nums:
                        try:
                            free_mig += int(float(nums[-1]))
                        except ValueError:
                            pass
                    break

    # ── Timestamp ─────────────────────────────────────────────────────────────
    ts_match  = re.search(r'Cluster status\s+([\w,: +]+\d{4})', text)
    timestamp = ts_match.group(1).strip() if ts_match else None

    return dict(
        studentGPU=student_gpu,
        researcherGPU=researcher_gpu,
        studentMIG=student_mig,
        researcherMIG=researcher_mig,
        freeMIG=free_mig,
        studentLimit=STUDENT_LIMIT,
        researcherLimit=RESEARCHER_LIMIT,
        migLimit=MIG_LIMIT,
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
