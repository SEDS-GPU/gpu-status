#!/usr/bin/env python3
"""
Reads users.csv and scrapes the cluster status page.
Writes status.json with only counts — no usernames ever leave this script.

Only the "Resource usage (overall)" tree is parsed. Each resource there is a
top-level line (e.g. " nvidia.com/gpu  (92%) 55.0 ..."), node lines carry a
percentage ("├─ kiaransalee  (83%) 5.0 ..."), pod lines do not.

Counting rules:
- nvidia.com/gpu -> kiaransalee: all pods except system ones count.
  GPU count is the pod's "Requested" value.
  jupyter-USERNAME: look up CSV, unknown = researcher.
  manually named pods: look up CSV, unknown = researcher.
- nvidia.com/mig-* -> kiaransalee: jupyter-* pods only.
  look up CSV, unknown = student.
- Used MIG slices = sum of the kiaransalee node's Requested column over all
  nvidia.com/mig-* resources (every pod, Jupyter or not).
  Total = MIG_LIMIT (the page's Allocatable column under-reports it);
  free = MIG_LIMIT - used.
"""

import csv
import html
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
TREE_CHARS      = '│├└─'


def load_users(path='users.csv'):
    users = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for row in reader:
            username = (row.get('github account') or '').strip().lower()
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


# ── Page → plain text lines ──────────────────────────────────────────────────
def to_lines(page):
    text = re.sub(r'<br\s*/?>', '\n', page, flags=re.IGNORECASE)
    text = re.sub(r'<(?:tr|p|div|li|pre)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?(?:td|th)[^>]*>', '  ', text, flags=re.IGNORECASE)   # table cells -> gap
    text = re.sub(r'<[^>]+>', '', text)
    # Decode &#9474; / &nbsp; etc. into real characters so entity codes can never
    # be mistaken for numbers (this is what produced "9473 MIG slices").
    text = html.unescape(text)
    # Normalise non-breaking and other unicode spaces to plain spaces.
    text = re.sub(r'[\u00a0\u2000-\u200b\u202f\u205f\u3000\t]', ' ', text)
    return text.splitlines()


def split_row(line):
    """'│  ├─ jupyter-x   1.0   1.0' -> ('jupyter-x', [1.0, 1.0], has_percent)"""
    has_pct = bool(re.search(r'\(\s*\d+(?:\.\d+)?\s*%\s*\)', line))
    body  = re.sub(r'\(\s*\d+(?:\.\d+)?\s*%\s*\)', ' ', line)     # drop "(83%)"
    body  = re.sub('[' + TREE_CHARS + ']', ' ', body)
    parts = body.split()
    if not parts:
        return None, [], False
    name, nums = parts[0], []
    for tok in parts[1:]:
        try:
            nums.append(float(tok))
        except ValueError:
            pass                         # "1.0Gi", "500.0m" etc. — not needed here
    return name, nums, has_pct


def parse_tree(lines):
    """
    Returns {resource: {node: {'requested', 'alloc', 'free', 'pods': {name: req}}}}
    for nvidia.com/* resources of the overall usage tree only.
    """
    resources = {}
    cur_res = cur_node = None

    for line in lines:
        if not line.strip():
            continue                      # blank lines (e.g. from <br>) carry no info
        has_tree = any(c in line for c in TREE_CHARS)

        if not has_tree:
            # Top-level line: a new resource header, or something unrelated.
            m = re.match(r'^\s*(nvidia\.com/[\w.\-]+)', line)
            if m and m.group(1) not in resources:
                cur_res = m.group(1)
                resources[cur_res] = {}
            else:
                cur_res = None
            cur_node = None
            continue

        if cur_res is None:
            continue

        name, nums, is_node = split_row(line)
        if not name:
            continue

        if is_node:
            # Columns: Requested, Limit, Allocatable, Free
            cur_node = name.lower()
            resources[cur_res][cur_node] = dict(
                requested=nums[0] if len(nums) > 0 else 0.0,
                alloc=nums[2]     if len(nums) > 2 else 0.0,
                free=nums[3]      if len(nums) > 3 else 0.0,
                pods={},
            )
        elif cur_node is not None:
            # Columns: Requested, Limit
            resources[cur_res][cur_node]['pods'][name.lower()] = nums[0] if nums else 1.0

    return resources


# ── User lookup ───────────────────────────────────────────────────────────────
def jupyter_user(pod):
    """jupyter-giordano-2ddemarzo -> giordano-demarzo (KubeSpawner escaping)."""
    u = pod[len('jupyter-'):]
    u = re.sub(r'---[0-9a-f]{6,}$', '', u)   # hashed safe-slug suffix
    u = u.split('--')[0]                      # named-server suffix
    return u.replace('-2d', '-').replace('-2e', '.').replace('-5f', '_')


def role_of(name, users):
    if name.startswith('jupyter-'):
        raw = name[len('jupyter-'):]
        return users.get(jupyter_user(name)) or users.get(raw)
    return users.get(name)


def is_system(name):
    return any(name.startswith(p) for p in SYSTEM_PREFIXES)


# ── Main computation ─────────────────────────────────────────────────────────
def parse_status(page, users):
    lines     = to_lines(page)
    resources = parse_tree(lines)

    # Safety net: never publish zeros because the page layout changed.
    if JUPYTER_NODE not in resources.get('nvidia.com/gpu', {}):
        # Only print resource header / node lines (no pod names = no usernames).
        for ln in lines:
            if 'nvidia.com/gpu' in ln or (JUPYTER_NODE in ln and '%' in ln):
                print('DEBUG', repr(ln[:160]))
        raise SystemExit('Could not find the kiaransalee row under nvidia.com/gpu; '
                         'status.json NOT updated. See DEBUG lines above.')

    student_gpu = researcher_gpu = 0
    student_mig = researcher_mig = 0
    used_mig = 0

    # GPU: every non-system pod on the Jupyter node
    node = resources.get('nvidia.com/gpu', {}).get(JUPYTER_NODE)
    if node:
        for pod, req in node['pods'].items():
            if is_system(pod):
                continue
            count = max(1, int(round(req)))
            if role_of(pod, users) == 'student':
                student_gpu += count
            else:
                researcher_gpu += count

    # MIG: all mig profiles on the Jupyter node
    for res, nodes in resources.items():
        if not res.startswith('nvidia.com/mig-'):
            continue
        node = nodes.get(JUPYTER_NODE)
        if not node:
            continue
        # Used = slices requested by pods on this node (incl. non-Jupyter pods).
        # Total/free are NOT taken from the page: its Allocatable column
        # under-reports kiaransalee's slices, so the total comes from MIG_LIMIT.
        used_mig += int(round(node['requested']))
        for pod, req in node['pods'].items():
            if not pod.startswith('jupyter-'):
                continue
            count = max(1, int(round(req)))
            if role_of(pod, users) == 'researcher':
                researcher_mig += count
            else:
                student_mig += count

    total_mig = MIG_LIMIT
    free_mig  = max(0, total_mig - used_mig)

    text      = '\n'.join(lines)
    ts_match  = re.search(r'Cluster status\s+([\w,: +]+\d{4})', text)
    timestamp = ts_match.group(1).strip() if ts_match else None

    return dict(
        studentGPU=student_gpu,
        researcherGPU=researcher_gpu,
        studentMIG=student_mig,
        researcherMIG=researcher_mig,
        freeMIG=free_mig,
        totalMIG=total_mig,
        usedMIG=used_mig,               # all pods, incl. non-Jupyter ones
        studentLimit=STUDENT_LIMIT,
        researcherLimit=RESEARCHER_LIMIT,
        migLimit=MIG_LIMIT,
        timestamp=timestamp,
        updatedAt=datetime.now(timezone.utc).isoformat(),
    )


if __name__ == '__main__':
    users  = load_users('users.csv')
    page   = fetch_status()
    result = parse_status(page, users)

    with open('status.json', 'w') as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))
