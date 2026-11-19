#!/usr/bin/env python3
"""A standalone GitHub-compatible git forge in one process (stdlib only).

Serves four surfaces from one in-memory store:
  - git smart HTTP (/<owner>/<repo>.git/info/refs, git-upload-pack, git-receive-pack)
  - GitHub REST v3 (/api/v3/...)
  - GitHub GraphQL v4 (/api/graphql)
  - Model Context Protocol over streamable HTTP (/mcp)

Lifecycle (the harness contract): bind an ephemeral port, POST {"port": N} to
$MAKEFUN_ENV_URL/register, then poll GET $MAKEFUN_ENV_URL/done until it answers
{"done": true} and exit 0. With --serve PORT it just serves (local testing).
"""
import base64
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.request
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------
# The seeded scenario (wire contract): the suite's users and labels derive
# from one constant seed through this exact RNG, so the forge's viewer is the
# scenario's viewer. Ported from the kit's wire.ts (xorshift64* + splitmix64).
# --------------------------------------------------------------------------

U64 = (1 << 64) - 1
SCENARIO_SEED = 0x6A09E667F3BCC909


def fnv1a64(text):
    h = 0xCBF29CE484222325
    for ch in text:
        h ^= ord(ch)
        h = (h * 0x100000001B3) & U64
    return h


def mix64(value):
    z = (value + 0x9E3779B97F4A7C15) & U64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & U64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & U64
    return (z ^ (z >> 31)) & U64


class Rng:
    def __init__(self, seed):
        s = seed & U64
        self.state = s if s != 0 else SCENARIO_SEED

    def next_u64(self):
        x = self.state
        x = (x ^ (x >> 12)) & U64
        x = (x ^ (x << 25)) & U64
        x = (x ^ (x >> 27)) & U64
        self.state = x
        return (x * 0x2545F4914F6CDD1D) & U64

    def next_u32(self):
        return self.next_u64() >> 32

    def next_int(self, max_exclusive):
        if max_exclusive == 1:
            return 0
        limit = (1 << 32) - ((1 << 32) % max_exclusive)
        while True:
            v = self.next_u32()
            if v < limit:
                return v % max_exclusive

    def pick(self, items):
        return items[self.next_int(len(items))]


def scenario_rng(label):
    return Rng(mix64(SCENARIO_SEED ^ fnv1a64(label)))


FIRST_NAMES = ['ada', 'bram', 'cora', 'dmitri', 'elena', 'felix', 'greta', 'hugo', 'iris', 'jonas',
               'kira', 'lars', 'mira', 'nico', 'olga', 'pavel', 'quinn', 'rosa', 'sven', 'tess',
               'ulla', 'vito', 'wren', 'yara', 'zane']
LAST_NAMES = ['anders', 'bauer', 'costa', 'dahl', 'eriksen', 'fischer', 'garcia', 'holm', 'iversen',
              'jansen', 'keller', 'lind', 'moreau', 'novak', 'olsen', 'peters', 'quist', 'rossi',
              'sato', 'turi', 'ulrich', 'vega', 'weber', 'xu', 'yilmaz', 'zimmer']
ISSUE_LABELS = ['bug', 'enhancement', 'documentation', 'question', 'good first issue', 'help wanted']
LABEL_COLORS = {
    'bug': 'd73a4a', 'enhancement': 'a2eeef', 'documentation': '0075ca',
    'question': 'd876e3', 'good first issue': '7057ff', 'help wanted': '008672',
}


def generate_users(count):
    rng = scenario_rng('users')
    used = set()
    users = []
    for i in range(count):
        first = rng.pick(FIRST_NAMES)
        last = rng.pick(LAST_NAMES)
        base = re.sub(r'[^a-z0-9-]', '', first + last)
        login = base
        suffix = 2
        while login in used:
            login = f'{base}-{suffix}'
            suffix += 1
        used.add(login)
        users.append({
            'id': 1000 + i,
            'login': login,
            'name': f'{first} {last}',
            'email': f'{login}@example.invalid',
            'type': 'User',
        })
    return users


USERS = generate_users(12)
VIEWER = USERS[0]
USERS_BY_LOGIN = {u['login']: u for u in USERS}

# --------------------------------------------------------------------------
# The store. Everything lives in memory; one lock serialises mutations.
# --------------------------------------------------------------------------

_store_lock = threading.RLock()
_clock_lock = threading.Lock()
_clock = [int(time.time())]
_id_counter = [100000]
_repo_id_counter = [5000]


def tick():
    """A monotone logical clock (seconds), so created_at never goes backwards."""
    with _clock_lock:
        _clock[0] += 1
        return _clock[0]


def iso(sec):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(sec))


def next_id():
    with _clock_lock:
        _id_counter[0] += 1
        return _id_counter[0]


def next_repo_id():
    with _clock_lock:
        _repo_id_counter[0] += 1
        return _repo_id_counter[0]


def node_id(kind, raw_id):
    return base64.b64encode(f'{kind}:{raw_id}'.encode()).decode()


class Repo:
    def __init__(self, owner, name, description='', private=False):
        self.id = next_repo_id()
        self.owner = owner            # login string
        self.name = name
        self.description = description or ''
        self.private = bool(private)
        self.default_branch = 'main'
        now = tick()
        self.created_at = now
        self.updated_at = now
        self.pushed_at = now
        # git state
        self.objects = {}             # sha hex -> (type str, data bytes)
        self.refs = {}                # 'refs/heads/main' -> sha hex
        self.head = 'refs/heads/main' # symref target
        # forge state
        self.issues = {}              # number -> Issue dict (issues and pulls share numbers)
        self.next_number = 1
        self.labels = {name: {'id': next_id(), 'name': name, 'color': LABEL_COLORS[name],
                              'description': None} for name in ISSUE_LABELS}

    @property
    def full_name(self):
        return f'{self.owner}/{self.name}'


class Store:
    def __init__(self):
        self.repos = {}               # (owner, name) -> Repo

    def get(self, owner, name):
        return self.repos.get((owner, name))

    def create(self, owner, name, description='', private=False):
        repo = Repo(owner, name, description, private)
        self.repos[(owner, name)] = repo
        return repo

    def delete(self, owner, name):
        self.repos.pop((owner, name), None)

    def by_owner(self, owner):
        return [r for (o, _), r in sorted(self.repos.items()) if o == owner]

    def by_id(self, repo_id):
        for r in self.repos.values():
            if r.id == repo_id:
                return r
        return None


STORE = Store()

# --------------------------------------------------------------------------
# JSON shapes (GitHub REST v3)
# --------------------------------------------------------------------------


def user_json(u):
    return {
        'login': u['login'],
        'id': u['id'],
        'node_id': node_id('User', u['id']),
        'avatar_url': f"https://avatars.githubusercontent.com/u/{u['id']}?v=4",
        'gravatar_id': '',
        'url': f"/api/v3/users/{u['login']}",
        'html_url': f"/{u['login']}",
        'type': u.get('type', 'User'),
        'site_admin': False,
        'name': u.get('name'),
        'email': u.get('email'),
    }


def repo_json(r):
    owner = USERS_BY_LOGIN.get(r.owner) or {'login': r.owner, 'id': 0, 'name': None, 'email': None}
    return {
        'id': r.id,
        'node_id': node_id('Repository', r.id),
        'name': r.name,
        'full_name': r.full_name,
        'owner': user_json(owner) if owner.get('id') else {
            'login': r.owner, 'id': 0, 'node_id': node_id('User', 0), 'type': 'User'},
        'private': r.private,
        'html_url': f'/{r.full_name}',
        'description': r.description,
        'fork': False,
        'url': f'/api/v3/repos/{r.full_name}',
        'created_at': iso(r.created_at),
        'updated_at': iso(r.updated_at),
        'pushed_at': iso(r.pushed_at),
        'git_url': f'git:///{r.full_name}.git',
        'ssh_url': f'git@forge:{r.full_name}.git',
        'clone_url': f'/{r.full_name}.git',
        'homepage': None,
        'size': 0,
        'stargazers_count': 0,
        'watchers_count': 0,
        'language': None,
        'has_issues': True,
        'has_projects': True,
        'has_downloads': True,
        'has_wiki': True,
        'has_pages': False,
        'forks_count': 0,
        'open_issues_count': sum(1 for i in r.issues.values()
                                 if i['state'] == 'open' and not i.get('pull_request')),
        'license': {'key': 'mit', 'name': 'MIT License', 'spdx_id': 'MIT'},
        'default_branch': r.default_branch,
        'visibility': 'private' if r.private else 'public',
    }


def label_json(l):
    return {'id': l['id'], 'node_id': node_id('Label', l['id']), 'name': l['name'],
            'color': l['color'], 'default': True, 'description': l['description']}


def issue_json(r, i):
    out = {
        'id': i['id'],
        'node_id': node_id('Issue', i['id']),
        'url': f"/api/v3/repos/{r.full_name}/issues/{i['number']}",
        'repository_url': f'/api/v3/repos/{r.full_name}',
        'html_url': f"/{r.full_name}/issues/{i['number']}",
        'number': i['number'],
        'state': i['state'],
        'title': i['title'],
        'body': i.get('body'),
        'user': user_json(USERS_BY_LOGIN.get(i['user']) or VIEWER),
        'labels': [label_json(r.labels[n]) for n in i.get('labels', []) if n in r.labels],
        'assignee': None,
        'assignees': [],
        'comments': len(i.get('comments', [])),
        'created_at': iso(i['created_at']),
        'updated_at': iso(i['updated_at']),
        'closed_at': iso(i['closed_at']) if i.get('closed_at') else None,
    }
    if i.get('pull_request'):
        out['pull_request'] = {
            'url': f"/api/v3/repos/{r.full_name}/pulls/{i['number']}",
            'html_url': f"/{r.full_name}/pull/{i['number']}",
        }
    return out


def comment_json(r, i, c):
    return {
        'id': c['id'],
        'node_id': node_id('IssueComment', c['id']),
        'url': f"/api/v3/repos/{r.full_name}/issues/comments/{c['id']}",
        'html_url': f"/{r.full_name}/issues/{i['number']}#issuecomment-{c['id']}",
        'body': c['body'],
        'user': user_json(USERS_BY_LOGIN.get(c['user']) or VIEWER),
        'created_at': iso(c['created_at']),
        'updated_at': iso(c['updated_at']),
    }

# --------------------------------------------------------------------------
# Git object engine: objects, trees, commits, packs, ancestry, merges.
# --------------------------------------------------------------------------

OBJ_TYPES = {'commit': 1, 'tree': 2, 'blob': 3, 'tag': 4}
TYPE_NAMES = {v: k for k, v in OBJ_TYPES.items()}
ZERO_SHA = '0' * 40


def hash_object(type_name, data):
    return hashlib.sha1(f'{type_name} {len(data)}'.encode() + b'\0' + data).hexdigest()


def store_object(repo, type_name, data):
    sha = hash_object(type_name, data)
    repo.objects[sha] = (type_name, data)
    return sha


def get_object(repo, sha):
    return repo.objects.get(sha)


def parse_commit(data):
    head, _, message = data.partition(b'\n\n')
    out = {'tree': None, 'parents': [], 'author': None, 'committer': None,
           'message': message.decode('utf-8', 'replace'), 'extra': []}
    for line in head.split(b'\n'):
        if line.startswith(b'tree '):
            out['tree'] = line[5:].decode()
        elif line.startswith(b'parent '):
            out['parents'].append(line[7:].decode())
        elif line.startswith(b'author '):
            out['author'] = line[7:].decode('utf-8', 'replace')
        elif line.startswith(b'committer '):
            out['committer'] = line[10:].decode('utf-8', 'replace')
        else:
            out['extra'].append(line)
    return out


def build_commit(tree, parents, author, committer, message, extra=()):
    lines = [f'tree {tree}']
    lines += [f'parent {p}' for p in parents]
    lines.append(f'author {author}')
    lines.append(f'committer {committer}')
    lines += [e.decode('utf-8', 'replace') if isinstance(e, bytes) else e for e in extra]
    return ('\n'.join(lines) + '\n\n' + message).encode()


def parse_tree(data):
    entries = []
    i = 0
    while i < len(data):
        sp = data.index(b' ', i)
        mode = data[i:sp].decode()
        nul = data.index(b'\0', sp)
        name = data[sp + 1:nul].decode('utf-8', 'surrogateescape')
        sha = data[nul + 1:nul + 21].hex()
        entries.append((mode, name, sha))
        i = nul + 21
    return entries


def build_tree(entries):
    # entries: list of (mode, name, sha); git sorts trees with dirs as name+'/'
    def key(e):
        mode, name, _ = e
        return name + '/' if mode == '40000' else name
    out = b''
    for mode, name, sha in sorted(entries, key=key):
        out += f'{mode} '.encode() + name.encode('utf-8', 'surrogateescape') + b'\0' + bytes.fromhex(sha)
    return out


def flatten_tree(repo, tree_sha, prefix=''):
    """{path: (mode, sha)} for every blob under a tree (recursive)."""
    out = {}
    if tree_sha is None:
        return out
    obj = get_object(repo, tree_sha)
    if obj is None or obj[0] != 'tree':
        return out
    for mode, name, sha in parse_tree(obj[1]):
        path = prefix + name
        if mode == '40000':
            out.update(flatten_tree(repo, sha, path + '/'))
        else:
            out[path] = (mode, sha)
    return out


def build_tree_from_flat(repo, flat):
    """Store nested trees for a {path: (mode, sha)} map; return the root tree sha."""
    # group by directory
    dirs = {'': {}}
    for path, (mode, sha) in flat.items():
        parts = path.split('/')
        for i in range(1, len(parts)):
            dirs.setdefault('/'.join(parts[:i]), {})
        dirs['/'.join(parts[:-1])][parts[-1]] = (mode, sha)

    def emit(dirpath):
        entries = []
        for name, val in dirs.get(dirpath, {}).items():
            entries.append((val[0], name, val[1]))
        # subdirectories of dirpath
        for other in dirs:
            if other == dirpath or other == '':
                continue
            parent = '/'.join(other.split('/')[:-1])
            if parent == dirpath:
                entries.append(('40000', other.split('/')[-1], emit(other)))
        data = build_tree(entries)
        return store_object(repo, 'tree', data)

    return emit('')


def commit_tree_sha(repo, commit_sha):
    obj = get_object(repo, commit_sha)
    if obj is None or obj[0] != 'commit':
        return None
    return parse_commit(obj[1])['tree']


def ancestors(repo, tip):
    """All commit shas reachable from tip (including tip), BFS over parents."""
    seen = set()
    stack = [tip] if tip else []
    while stack:
        sha = stack.pop()
        if sha in seen or sha == ZERO_SHA:
            continue
        obj = get_object(repo, sha)
        if obj is None or obj[0] != 'commit':
            continue
        seen.add(sha)
        stack.extend(parse_commit(obj[1])['parents'])
    return seen


def is_ancestor(repo, anc, desc):
    if anc == desc:
        return True
    return anc in ancestors(repo, desc)


def merge_base(repo, a, b):
    anc_a = ancestors(repo, a)
    # BFS from b looking for the first commit that is an ancestor of a
    seen = set()
    queue = [b]
    while queue:
        sha = queue.pop(0)
        if sha in seen:
            continue
        seen.add(sha)
        if sha in anc_a:
            return sha
        obj = get_object(repo, sha)
        if obj and obj[0] == 'commit':
            queue.extend(parse_commit(obj[1])['parents'])
    return None


def reachable_objects(repo, tips, exclude_commits=frozenset(), shallow_boundary=frozenset()):
    """Every object (commit/tree/blob/tag) reachable from tips.

    Commits in exclude_commits (and their exclusive trees/blobs) are left out,
    as is anything below shallow_boundary (the boundary commits themselves are
    included, their parents are not walked).
    """
    out = {}
    commit_stack = [t for t in tips if t and t != ZERO_SHA]
    seen_commits = set()
    while commit_stack:
        sha = commit_stack.pop()
        if sha in seen_commits or sha in exclude_commits:
            continue
        obj = get_object(repo, sha)
        if obj is None or obj[0] != 'commit':
            continue
        seen_commits.add(sha)
        out[sha] = obj
        parsed = parse_commit(obj[1])
        if parsed['tree']:
            stack = [parsed['tree']]
            while stack:
                tsha = stack.pop()
                if tsha in out:
                    continue
                tobj = get_object(repo, tsha)
                if tobj is None:
                    continue
                out[tsha] = tobj
                if tobj[0] == 'tree':
                    for mode, _name, child in parse_tree(tobj[1]):
                        if mode == '40000':
                            stack.append(child)
                        else:
                            bobj = get_object(repo, child)
                            if bobj is not None:
                                out[child] = bobj
                elif tobj[0] == 'tag':
                    pass
        if sha not in shallow_boundary:
            commit_stack.extend(parsed['parents'])
    # annotated tags pointing at included commits
    for sha, (tname, data) in list(repo.objects.items()):
        if tname == 'tag' and sha not in out:
            m = re.search(rb'object ([0-9a-f]{40})', data)
            if m and m.group(1).decode() in out:
                out[sha] = (tname, data)
    return out


# ---- packfiles ------------------------------------------------------------


def _pack_varint(type_code, size):
    out = bytearray()
    first = (type_code << 4) | (size & 0x0F)
    size >>= 4
    if size:
        first |= 0x80
    out.append(first)
    while size:
        b = size & 0x7F
        size >>= 7
        if size:
            b |= 0x80
        out.append(b)
    return bytes(out)


def write_pack(objects):
    """objects: list of (sha, type_name, data). A valid v2 pack, no deltas."""
    out = bytearray()
    out += b'PACK' + (2).to_bytes(4, 'big') + len(objects).to_bytes(4, 'big')
    for _sha, type_name, data in objects:
        out += _pack_varint(OBJ_TYPES[type_name], len(data))
        out += zlib.compress(data)
    out += hashlib.sha1(bytes(out)).digest()
    return bytes(out)


def _read_varint_size(data, i):
    b = data[i]
    i += 1
    type_code = (b >> 4) & 0x07
    size = b & 0x0F
    shift = 4
    while b & 0x80:
        b = data[i]
        i += 1
        size |= (b & 0x7F) << shift
        shift += 7
    return type_code, size, i


def apply_delta(base, delta):
    def read_size(i):
        size = 0
        shift = 0
        while True:
            b = delta[i]
            i += 1
            size |= (b & 0x7F) << shift
            shift += 7
            if not (b & 0x80):
                return size, i
    src_size, i = read_size(0)
    assert src_size == len(base), 'delta base size mismatch'
    _tgt_size, i = read_size(i)
    out = bytearray()
    while i < len(delta):
        op = delta[i]
        i += 1
        if op & 0x80:
            offset = 0
            size = 0
            for bit in range(4):
                if op & (1 << bit):
                    offset |= delta[i] << (8 * bit)
                    i += 1
            for bit in range(3):
                if op & (0x10 << bit):
                    size |= delta[i] << (8 * bit)
                    i += 1
            if size == 0:
                size = 0x10000
            out += base[offset:offset + size]
        elif op:
            out += delta[i:i + op]
            i += op
        else:
            raise ValueError('delta opcode 0')
    return bytes(out)


def read_pack(repo, data):
    """Parse a pack into repo's object store. Returns the list of stored shas.

    Handles ofs-delta and ref-delta (ref bases may live in the pack or, for a
    thin pack, in the repo already)."""
    if len(data) < 12 or data[:4] != b'PACK':
        raise ValueError('not a pack')
    count = int.from_bytes(data[8:12], 'big')
    i = 12
    entries = []  # (offset, type_code, payload) payload: raw data or delta info
    by_offset = {}
    for _ in range(count):
        offset = i
        type_code, size, i = _read_varint_size(data, i)
        if type_code == 6:  # ofs-delta
            b = data[i]
            i += 1
            back = b & 0x7F
            while b & 0x80:
                b = data[i]
                i += 1
                back = ((back + 1) << 7) | (b & 0x7F)
            base_offset = offset - back
            entries.append((offset, type_code, base_offset))
            by_offset[offset] = len(entries) - 1
            d = zlib.decompressobj()
            raw = d.decompress(data[i:])
            i += len(data[i:]) - len(d.unused_data)
            entries[-1] = (offset, type_code, (base_offset, raw))
        elif type_code == 7:  # ref-delta
            base_sha = data[i:i + 20].hex()
            i += 20
            d = zlib.decompressobj()
            raw = d.decompress(data[i:])
            i += len(data[i:]) - len(d.unused_data)
            entries.append((offset, type_code, (base_sha, raw)))
            by_offset[offset] = len(entries) - 1
        else:
            d = zlib.decompressobj()
            raw = d.decompress(data[i:])
            i += len(data[i:]) - len(d.unused_data)
            entries.append((offset, type_code, raw))
            by_offset[offset] = len(entries) - 1
    resolved = {}

    def resolve(idx):
        if idx in resolved:
            return resolved[idx]
        _offset, type_code, payload = entries[idx]
        if type_code in OBJ_TYPES.values():
            type_name = TYPE_NAMES[type_code]
            result = (type_name, payload)
        elif type_code == 6:
            base_offset, delta = payload
            base_type, base_data = resolve(by_offset[base_offset])
            result = (base_type, apply_delta(base_data, delta))
        else:
            base_sha, delta = payload
            base = None
            for j, (o2, t2, p2) in enumerate(entries):
                if t2 in OBJ_TYPES.values():
                    if hash_object(TYPE_NAMES[t2], p2) == base_sha:
                        base = resolve(j)
                        break
            if base is None:
                existing = get_object(repo, base_sha)
                if existing is None:
                    raise ValueError(f'ref-delta base {base_sha} not found')
                base = existing
            result = (base[0], apply_delta(base[1], delta))
        resolved[idx] = result
        return result

    shas = []
    for idx in range(len(entries)):
        type_name, raw = resolve(idx)
        shas.append(store_object(repo, type_name, raw))
    return shas


# ---- pkt-line -------------------------------------------------------------


def pkt(payload):
    if isinstance(payload, str):
        payload = payload.encode()
    return f'{len(payload) + 4:04x}'.encode() + payload


FLUSH = b'0000'
DELIM = b'0001'


def parse_pkt_lines(data, pos=0):
    """Yield (payload bytes or None for flush, 'delim' for delimiter), advancing pos."""
    while pos + 4 <= len(data):
        try:
            n = int(data[pos:pos + 4], 16)
        except ValueError:
            return
        if n == 0:
            yield None, pos + 4
            pos += 4
            return
        if n == 1:
            yield 'delim', pos + 4
            pos += 4
            continue
        if n == 2:
            yield 'response-end', pos + 4
            pos += 4
            continue
        yield data[pos + 4:pos + n], pos + n
        pos = pos + n


def read_pkt_stream(data):
    """Parse a whole pkt-line stream into a list of payloads (None = flush)."""
    out = []
    pos = 0
    while pos + 4 <= len(data):
        try:
            n = int(data[pos:pos + 4], 16)
        except ValueError:
            break
        if n == 0:
            out.append(None)
            pos += 4
            break
        if n in (1, 2):
            out.append('delim' if n == 1 else 'response-end')
            pos += 4
            continue
        out.append(data[pos + 4:pos + n])
        pos += n
    return out, data[pos:]

# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------

TOKEN = os.environ.get('MAKEFUN_SHOP_TOKEN') or None
_req_counter = [0]


def request_id():
    with _clock_lock:
        _req_counter[0] += 1
        return f'{_req_counter[0]:08X}'


class Ctx:
    def __init__(self, handler, method, path, query, body, headers):
        self.handler = handler
        self.method = method
        self.path = path
        self.query = query          # dict[str, list[str]]
        self.body = body            # bytes
        self.headers = headers
        self.json = None
        if body:
            try:
                self.json = json.loads(body.decode('utf-8'))
            except Exception:
                self.json = None

    def q(self, name, default=None):
        values = self.query.get(name)
        return values[0] if values else default

    def send(self, status, obj=None, headers=None, raw=None, content_type='application/json; charset=utf-8'):
        if raw is not None:
            data = raw
        elif obj is not None:
            data = (json.dumps(obj) + '\n').encode()
        else:
            data = b''
        h = {'content-type': content_type, 'content-length': str(len(data)),
             'x-github-request-id': request_id(), 'x-request-id': request_id()}
        if headers:
            h.update(headers)
        self.handler.send_response(status)
        for k, v in h.items():
            self.handler.send_header(k, v)
        self.handler.end_headers()
        if self.method != 'HEAD':
            self.handler.wfile.write(data)

    def error(self, status, message, extra=None):
        body = {'message': message}
        if extra:
            body.update(extra)
        self.send(status, body)

    def send_raw(self, status, data, content_type):
        self.send(status, raw=data, content_type=content_type)


def paginate(ctx, items):
    """GitHub-style page/per_page slicing plus a Link header."""
    try:
        per_page = int(ctx.q('per_page', '30'))
    except ValueError:
        per_page = 30
    try:
        page = int(ctx.q('page', '1'))
    except ValueError:
        page = 1
    per_page = max(1, min(per_page, 100))
    page = max(1, page)
    total = len(items)
    start = (page - 1) * per_page
    sliced = items[start:start + per_page]
    host = ctx.headers.get('Host', '127.0.0.1')
    base = f'http://{host}{ctx.path}'
    params = {k: v[0] for k, v in ctx.query.items() if k not in ('page', 'per_page')}

    def url_for(p):
        qs = dict(params)
        qs['page'] = str(p)
        qs['per_page'] = str(per_page)
        query = '&'.join(f'{k}={v}' for k, v in sorted(qs.items()))
        return f'{base}?{query}'

    last_page = max(1, (total + per_page - 1) // per_page)
    links = []
    if page < last_page:
        links.append(f'<{url_for(page + 1)}>; rel="next"')
        links.append(f'<{url_for(last_page)}>; rel="last"')
    if page > 1:
        links.append(f'<{url_for(page - 1)}>; rel="prev"')
        links.append(f'<{url_for(1)}>; rel="first"')
    headers = {'link': ', '.join(links)} if links else {}
    return sliced, headers


def authed(ctx):
    if TOKEN is None:
        return True
    return ctx.headers.get('Authorization') == f'Bearer {TOKEN}'


# --------------------------------------------------------------------------
# REST v3: users and repositories
# --------------------------------------------------------------------------


def api_root(ctx):
    root = {
        'current_user_url': '/api/v3/user',
        'current_user_repositories_url': '/api/v3/user/repos{?type,page,per_page,sort}',
        'repository_url': '/api/v3/repos/{owner}/{repo}',
        'issues_url': '/api/v3/issues',
        'rate_limit_url': '/api/v3/rate_limit',
        'emojis_url': '/api/v3/emojis',
        'user_url': '/api/v3/users/{user}',
        'organization_url': '/api/v3/orgs/{org}',
    }
    ctx.send(200, root, headers={'x-oauth-scopes': 'repo, read:org'})


def get_user(ctx):
    ctx.send(200, user_json(VIEWER))


def get_user_by_login(ctx, login):
    u = USERS_BY_LOGIN.get(login)
    if u is None:
        return ctx.error(404, 'Not Found')
    ctx.send(200, user_json(u))


def create_repo(ctx):
    body = ctx.json or {}
    name = body.get('name')
    if not name or not isinstance(name, str):
        return ctx.error(422, 'Repository creation failed.',
                         {'errors': [{'resource': 'Repository', 'field': 'name', 'code': 'missing_field'}]})
    with _store_lock:
        if STORE.get(VIEWER['login'], name) is not None:
            return ctx.error(422, 'Repository creation failed.',
                             {'errors': [{'resource': 'Repository', 'field': 'name', 'code': 'custom',
                                          'message': 'name already exists on this account'}]})
        repo = STORE.create(VIEWER['login'], name, body.get('description') or '',
                            body.get('private', False))
        if body.get('auto_init'):
            _seed_initial_commit(repo)
        ctx.send(201, repo_json(repo))


def _seed_initial_commit(repo):
    flat = {}
    readme = f"# {repo.name}\n".encode()
    sha = store_object(repo, 'blob', readme)
    flat['README.md'] = ('100644', sha)
    tree = build_tree_from_flat(repo, flat)
    now = tick()
    ident = f"{VIEWER['name']} <{VIEWER['email']}> {now} +0000"
    commit = build_commit(tree, [], ident, ident, 'Initial commit')
    csha = store_object(repo, 'commit', commit)
    repo.refs[repo.head] = csha


def list_user_repos(ctx):
    with _store_lock:
        repos = STORE.by_owner(VIEWER['login'])
        items = [repo_json(r) for r in repos]
    sliced, headers = paginate(ctx, items)
    ctx.send(200, sliced, headers=headers)


def get_repo(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    ctx.send(200, repo_json(repo))


def patch_repo(ctx, owner, name):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        if 'description' in body:
            repo.description = body['description'] or ''
        if 'private' in body:
            repo.private = bool(body['private'])
        if 'default_branch' in body and body['default_branch']:
            repo.default_branch = body['default_branch']
            repo.head = f"refs/heads/{body['default_branch']}"
        if 'homepage' in body:
            pass
        new_name = body.get('name')
        if new_name and new_name != name:
            if STORE.get(owner, new_name) is not None:
                return ctx.error(422, 'Repository creation failed.',
                                 {'errors': [{'resource': 'Repository', 'field': 'name',
                                              'code': 'custom', 'message': 'name already exists on this account'}]})
            del STORE.repos[(owner, name)]
            repo.name = new_name
            STORE.repos[(owner, new_name)] = repo
        repo.updated_at = tick()
        ctx.send(200, repo_json(repo))


def delete_repo(ctx, owner, name):
    with _store_lock:
        if STORE.get(owner, name) is None:
            return ctx.error(404, 'Not Found')
        STORE.delete(owner, name)
    ctx.send(204)


def list_branches(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    out = []
    for ref in sorted(repo.refs):
        if ref.startswith('refs/heads/'):
            out.append({'name': ref[len('refs/heads/'):],
                        'commit': {'sha': repo.refs[ref],
                                   'url': f'/api/v3/repos/{repo.full_name}/commits/{repo.refs[ref]}'},
                        'protected': False})
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def get_branch(ctx, owner, name, branch):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    sha = repo.refs.get(f'refs/heads/{branch}')
    if sha is None:
        return ctx.error(404, 'Not Found')
    ctx.send(200, {'name': branch,
                   'commit': commit_json(repo, sha),
                   'protected': False})


def _parse_ident(line):
    m = re.match(r'(.*?) <(.*?)> (\d+) ([+-]\d{4})', line or '')
    if not m:
        return {'name': '', 'email': '', 'date': iso(tick())}
    name, email, ts, _tz = m.groups()
    return {'name': name, 'email': email, 'date': iso(int(ts))}


def commit_json(repo, sha):
    obj = get_object(repo, sha)
    if obj is None or obj[0] != 'commit':
        return None
    c = parse_commit(obj[1])
    author = _parse_ident(c['author'])
    committer = _parse_ident(c['committer'])

    def linked(ident):
        for u in USERS:
            if u['email'] == ident['email']:
                return user_json(u)
        return None

    return {
        'sha': sha,
        'node_id': node_id('Commit', sha),
        'commit': {
            'author': author,
            'committer': committer,
            'message': c['message'],
            'tree': {'sha': c['tree'], 'url': f'/api/v3/repos/{repo.full_name}/git/trees/{c["tree"]}'},
            'url': f'/api/v3/repos/{repo.full_name}/git/commits/{sha}',
            'comment_count': 0,
        },
        'url': f'/api/v3/repos/{repo.full_name}/commits/{sha}',
        'html_url': f'/{repo.full_name}/commit/{sha}',
        'comments_url': f'/api/v3/repos/{repo.full_name}/commits/{sha}/comments',
        'author': linked(author),
        'committer': linked(committer),
        'parents': [{'sha': p, 'url': f'/api/v3/repos/{repo.full_name}/commits/{p}',
                     'html_url': f'/{repo.full_name}/commit/{p}'} for p in c['parents']],
    }


def list_commits(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    ref = ctx.q('sha') or repo.default_branch
    tip = repo.refs.get(f'refs/heads/{ref}') or (ref if get_object(repo, ref or '') else None)
    if tip is None:
        return ctx.send(200, [])
    # newest first: BFS ordered by committer time descending
    seen = {}
    stack = [tip]
    while stack:
        sha = stack.pop()
        if sha in seen:
            continue
        obj = get_object(repo, sha)
        if obj is None or obj[0] != 'commit':
            continue
        seen[sha] = obj
        stack.extend(parse_commit(obj[1])['parents'])

    def commit_time(item):
        sha, obj = item
        ident = _parse_ident(parse_commit(obj[1])['committer'])
        return ident['date']

    ordered = [sha for sha, _ in sorted(seen.items(), key=commit_time, reverse=True)]
    items = [commit_json(repo, sha) for sha in ordered]
    sliced, headers = paginate(ctx, items)
    ctx.send(200, sliced, headers=headers)


def get_commit(ctx, owner, name, sha):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    out = commit_json(repo, sha)
    if out is None:
        return ctx.error(404, 'Not Found')
    ctx.send(200, out)


# --------------------------------------------------------------------------
# REST v3: contents
# --------------------------------------------------------------------------


def _resolve_ref(repo, ref):
    if ref is None:
        ref = repo.default_branch
    if ref in repo.refs:
        return repo.refs[ref]
    sha = repo.refs.get(f'refs/heads/{ref}') or repo.refs.get(f'refs/tags/{ref}')
    if sha:
        obj = get_object(repo, sha)
        if obj and obj[0] == 'tag':
            m = re.search(rb'object ([0-9a-f]{40})', obj[1])
            return m.group(1).decode() if m else sha
        return sha
    if get_object(repo, ref) and get_object(repo, ref)[0] == 'commit':
        return ref
    return None


def _flat_at(repo, ref):
    commit_sha = _resolve_ref(repo, ref)
    if commit_sha is None:
        return None, {}
    tree = commit_tree_sha(repo, commit_sha)
    return commit_sha, flatten_tree(repo, tree)


def _content_entry(repo, path, mode, sha):
    obj = get_object(repo, sha)
    size = len(obj[1]) if obj else 0
    name = path.split('/')[-1]
    if mode == '40000':
        return {'name': name, 'path': path, 'sha': sha, 'size': 0, 'type': 'dir',
                'url': f'/api/v3/repos/{repo.full_name}/contents/{path}',
                'html_url': f'/{repo.full_name}/tree/{repo.default_branch}/{path}',
                'git_url': f'/api/v3/repos/{repo.full_name}/git/trees/{sha}',
                'download_url': None, '_links': {}}
    return {'name': name, 'path': path, 'sha': sha, 'size': size, 'type': 'file',
            'url': f'/api/v3/repos/{repo.full_name}/contents/{path}',
            'html_url': f'/{repo.full_name}/blob/{repo.default_branch}/{path}',
            'git_url': f'/api/v3/repos/{repo.full_name}/git/blobs/{sha}',
            'download_url': f'/raw/{repo.full_name}/{repo.default_branch}/{path}', '_links': {}}


def get_contents(ctx, owner, name, path):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    ref = ctx.q('ref')
    commit_sha, flat = _flat_at(repo, ref)
    if commit_sha is None:
        if not repo.refs:
            return ctx.send(200, [])
        return ctx.error(404, 'Not Found')
    path = path.strip('/')
    if path == '':
        entries = []
        seen_dirs = set()
        for p, (mode, sha) in sorted(flat.items()):
            head = p.split('/')[0]
            if '/' in p:
                if head not in seen_dirs:
                    seen_dirs.add(head)
                    tree = commit_tree_sha(repo, commit_sha)
                    for m2, n2, s2 in parse_tree(get_object(repo, tree)[1]):
                        if n2 == head and m2 == '40000':
                            entries.append(_content_entry(repo, head, m2, s2))
            else:
                entries.append(_content_entry(repo, p, mode, sha))
        return ctx.send(200, entries)
    if path in flat:
        mode, sha = flat[path]
        entry = _content_entry(repo, path, mode, sha)
        data = get_object(repo, sha)[1]
        entry['encoding'] = 'base64'
        entry['content'] = base64.b64encode(data).decode()
        return ctx.send(200, entry)
    # directory?
    prefix = path + '/'
    if any(p.startswith(prefix) for p in flat):
        tree = commit_tree_sha(repo, commit_sha)
        # walk to the subtree
        parts = path.split('/')
        cur = tree
        for part in parts:
            nxt = None
            for m2, n2, s2 in parse_tree(get_object(repo, cur)[1]):
                if n2 == part and m2 == '40000':
                    nxt = s2
                    break
            if nxt is None:
                return ctx.error(404, 'Not Found')
            cur = nxt
        entries = [_content_entry(repo, f'{path}/{n2}', m2, s2)
                   for m2, n2, s2 in parse_tree(get_object(repo, cur)[1])]
        return ctx.send(200, entries)
    return ctx.error(404, 'Not Found')


def _commit_flat(repo, flat, branch, message, parents):
    tree = build_tree_from_flat(repo, flat)
    now = tick()
    ident = f"{VIEWER['name']} <{VIEWER['email']}> {now} +0000"
    data = build_commit(tree, parents, ident, ident, message)
    sha = store_object(repo, 'commit', data)
    repo.refs[f'refs/heads/{branch}'] = sha
    if branch == repo.default_branch:
        repo.head = f'refs/heads/{branch}'
    repo.pushed_at = now
    repo.updated_at = now
    return sha


def put_contents(ctx, owner, name, path):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        message = body.get('message')
        content_b64 = body.get('content')
        if message is None or content_b64 is None:
            return ctx.error(422, 'Invalid request.',
                             {'errors': [{'resource': 'Contents', 'code': 'missing_field'}]})
        try:
            data = base64.b64decode(re.sub(r'\s+', '', content_b64))
        except Exception:
            return ctx.error(422, 'content is not valid base64')
        branch = body.get('branch') or repo.default_branch
        refname = f'refs/heads/{branch}'
        tip = repo.refs.get(refname)
        _cs, flat = _flat_at(repo, branch) if tip else (None, {})
        path = path.strip('/')
        existing = flat.get(path)
        given_sha = body.get('sha')
        if existing is not None:
            if given_sha is None:
                return ctx.error(422, '"sha" wasn\'t supplied.', {'errors': [
                    {'resource': 'Contents', 'field': 'sha', 'code': 'missing_field'}]})
            if given_sha != existing[1]:
                return ctx.error(409, f'{path} does not match {given_sha}')
            status = 200
        else:
            if given_sha is not None:
                return ctx.error(422, '"sha" must not be supplied for a new file.')
            status = 201
        blob_sha = store_object(repo, 'blob', data)
        flat[path] = ('100644', blob_sha)
        parents = [tip] if tip else []
        commit_sha = _commit_flat(repo, flat, branch, message, parents)
        entry = _content_entry(repo, path, '100644', blob_sha)
        entry['encoding'] = 'base64'
        entry['content'] = base64.b64encode(data).decode()
        ctx.send(status, {'content': entry,
                          'commit': {'sha': commit_sha, 'message': message,
                                     'url': f'/api/v3/repos/{repo.full_name}/git/commits/{commit_sha}'}})


def delete_contents(ctx, owner, name, path):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        message = body.get('message')
        given_sha = body.get('sha')
        if message is None or given_sha is None:
            return ctx.error(422, 'Invalid request.',
                             {'errors': [{'resource': 'Contents', 'code': 'missing_field'}]})
        branch = body.get('branch') or repo.default_branch
        refname = f'refs/heads/{branch}'
        tip = repo.refs.get(refname)
        if tip is None:
            return ctx.error(404, 'Not Found')
        _cs, flat = _flat_at(repo, branch)
        path = path.strip('/')
        existing = flat.get(path)
        if existing is None:
            return ctx.error(404, 'Not Found')
        if given_sha != existing[1]:
            return ctx.error(409, f'{path} does not match {given_sha}')
        del flat[path]
        commit_sha = _commit_flat(repo, flat, branch, message, [tip])
        ctx.send(200, {'content': None,
                       'commit': {'sha': commit_sha, 'message': message,
                                  'url': f'/api/v3/repos/{repo.full_name}/git/commits/{commit_sha}'}})


# --------------------------------------------------------------------------
# REST v3: issues and comments
# --------------------------------------------------------------------------


def _new_issue(repo, title, body, labels, is_pull=False, pull=None):
    number = repo.next_number
    repo.next_number += 1
    now = tick()
    issue = {
        'id': next_id(),
        'number': number,
        'title': title,
        'body': body,
        'state': 'open',
        'labels': list(labels or []),
        'user': VIEWER['login'],
        'comments': [],
        'created_at': now,
        'updated_at': now,
        'closed_at': None,
        'pull_request': bool(is_pull),
        'pull': pull,
    }
    repo.issues[number] = issue
    return issue


def _label_error(ctx):
    return ctx.error(422, 'Validation Failed', {'errors': [
        {'resource': 'Issue', 'field': 'labels', 'code': 'invalid'}]})


def create_issue(ctx, owner, name):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        title = body.get('title')
        if not title:
            return ctx.error(422, 'Validation Failed', {'errors': [
                {'resource': 'Issue', 'field': 'title', 'code': 'missing_field'}]})
        labels = body.get('labels') or []
        for lab in labels:
            if lab not in repo.labels:
                return _label_error(ctx)
        issue = _new_issue(repo, title, body.get('body'), labels)
        ctx.send(201, issue_json(repo, issue))


def list_issues(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    state = ctx.q('state', 'open')
    items = [i for i in repo.issues.values() if not i.get('pull_request')]
    if state == 'open':
        items = [i for i in items if i['state'] == 'open']
    elif state == 'closed':
        items = [i for i in items if i['state'] == 'closed']
    sort = ctx.q('sort', 'created')
    direction = ctx.q('direction', 'desc')
    key = (lambda i: i['created_at']) if sort == 'created' else (lambda i: i['updated_at'])
    items.sort(key=key, reverse=(direction != 'asc'))
    out = [issue_json(repo, i) for i in items]
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def get_issue(ctx, owner, name, number):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    issue = repo.issues.get(int(number))
    if issue is None:
        return ctx.error(404, 'Not Found')
    ctx.send(200, issue_json(repo, issue))


def patch_issue(ctx, owner, name, number):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        issue = repo.issues.get(int(number))
        if issue is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        if 'labels' in body and body['labels'] is not None:
            for lab in body['labels']:
                if lab not in repo.labels:
                    return _label_error(ctx)
            issue['labels'] = list(body['labels'])
        if 'title' in body:
            issue['title'] = body['title']
        if 'body' in body:
            issue['body'] = body['body']
        if 'state' in body:
            issue['state'] = body['state']
            issue['closed_at'] = tick() if body['state'] == 'closed' else None
        issue['updated_at'] = tick()
        ctx.send(200, issue_json(repo, issue))


def create_comment(ctx, owner, name, number):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        issue = repo.issues.get(int(number))
        if issue is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        if not body.get('body'):
            return ctx.error(422, 'Validation Failed', {'errors': [
                {'resource': 'IssueComment', 'field': 'body', 'code': 'missing_field'}]})
        now = tick()
        comment = {'id': next_id(), 'body': body['body'], 'user': VIEWER['login'],
                   'created_at': now, 'updated_at': now}
        issue['comments'].append(comment)
        issue['updated_at'] = now
        ctx.send(201, comment_json(repo, issue, comment))


def list_comments(ctx, owner, name, number):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    issue = repo.issues.get(int(number))
    if issue is None:
        return ctx.error(404, 'Not Found')
    out = [comment_json(repo, issue, c) for c in issue['comments']]
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def list_labels(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    out = [label_json(l) for l in repo.labels.values()]
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def create_label(ctx, owner, name):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        lname = body.get('name')
        if not lname:
            return ctx.error(422, 'Validation Failed', {'errors': [
                {'resource': 'Label', 'field': 'name', 'code': 'missing_field'}]})
        existing = repo.labels.get(lname)
        if existing is not None:
            return ctx.send(201, label_json(existing))
        label = {'id': next_id(), 'name': lname,
                 'color': (body.get('color') or 'ededed').lstrip('#'),
                 'description': body.get('description')}
        repo.labels[lname] = label
        ctx.send(201, label_json(label))


def api_meta(ctx):
    ctx.send(200, {
        'verifiable_password_authentication': False,
        'ssh_key_fingerprints': {},
        'hooks': [],
        'github_services_sha': 'forge',
        'installed_version': '1.0.0-forge',
    })


# --------------------------------------------------------------------------
# REST v3: git refs API (branch creation through the API)
# --------------------------------------------------------------------------


def _ref_json(repo, refname):
    sha = repo.refs[refname]
    obj = get_object(repo, sha)
    return {'ref': refname,
            'node_id': node_id('Ref', refname),
            'url': f'/api/v3/repos/{repo.full_name}/git/refs/{refname[5:]}',
            'object': {'type': obj[0] if obj else 'commit', 'sha': sha,
                       'url': f'/api/v3/repos/{repo.full_name}/git/commits/{sha}'}}


def create_ref(ctx, owner, name):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        ref = body.get('ref') or ''
        sha = body.get('sha') or ''
        if not ref.startswith('refs/'):
            return ctx.error(422, 'Invalid request.', {'errors': [
                {'resource': 'Reference', 'field': 'ref', 'code': 'custom',
                 'message': 'ref must start with refs/'}]})
        if get_object(repo, sha) is None:
            return ctx.error(422, 'Object does not exist')
        if ref in repo.refs:
            return ctx.error(422, 'Reference already exists')
        repo.refs[ref] = sha
        ctx.send(201, _ref_json(repo, ref))


def get_ref(ctx, owner, name, ref):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    refname = f'refs/{ref}'
    if refname not in repo.refs:
        return ctx.error(404, 'Not Found')
    ctx.send(200, _ref_json(repo, refname))


def delete_ref(ctx, owner, name, ref):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        refname = f'refs/{ref}'
        if refname not in repo.refs:
            return ctx.error(404, 'Not Found')
        del repo.refs[refname]
    ctx.send(204)

# --------------------------------------------------------------------------
# Surfaces implemented in later sections (git, pulls, GraphQL, MCP).
# These placeholders are replaced as each surface lands.
# --------------------------------------------------------------------------


AGENT = 'forge/1.0'


def _peel(repo, sha):
    """Follow tag objects to the commit they name; None when not a tag chain."""
    seen = set()
    cur = sha
    while cur not in seen:
        seen.add(cur)
        obj = get_object(repo, cur)
        if obj is None:
            return None
        if obj[0] == 'commit':
            return cur
        if obj[0] != 'tag':
            return None
        m = re.search(rb'object ([0-9a-f]{40})', obj[1])
        if not m:
            return None
        cur = m.group(1).decode()
    return None


def _is_tag_obj(repo, sha):
    obj = get_object(repo, sha)
    return obj is not None and obj[0] == 'tag'


def _advertise_v0(repo, service):
    if service == 'git-upload-pack':
        caps = (f'side-band-64k thin-pack ofs-delta shallow agent={AGENT} '
                f'symref=HEAD:{repo.head} object-format=sha1')
    else:
        caps = (f'report-status delete-refs side-band-64k ofs-delta agent={AGENT} '
                f'symref=HEAD:{repo.head}')
    out = bytearray()
    out += pkt(f'# service={service}\n')
    out += FLUSH
    if not repo.refs:
        out += pkt(f'{ZERO_SHA} capabilities^{{}}\0{caps}\n')
        out += FLUSH
        return bytes(out)
    head_sha = repo.refs.get(repo.head, ZERO_SHA)
    lines = [f'{head_sha} HEAD\0{caps}\n']
    for ref in sorted(repo.refs):
        sha = repo.refs[ref]
        lines.append(f'{sha} {ref}\n')
        if _is_tag_obj(repo, sha):
            peeled = _peel(repo, sha)
            if peeled:
                lines.append(f'{peeled} {ref}^{{}}\n')
    for line in lines:
        out += pkt(line)
    out += FLUSH
    return bytes(out)


def _advertise_v2():
    out = bytearray()
    out += pkt('version 2\n')
    out += pkt(f'agent={AGENT}\n')
    out += pkt('ls-refs=unborn\n')
    out += pkt('fetch=shallow filter\n')
    out += pkt('object-format=sha1\n')
    out += FLUSH
    return bytes(out)


def _sideband_packets(pack):
    out = bytearray()
    for i in range(0, len(pack), 32768):
        out += pkt(b'\x01' + pack[i:i + 32768])
    return bytes(out)


def _fetch_objects(repo, wants, haves, client_shallow, boundary):
    """(pack objects dict, new boundary set) for one fetch."""
    had = reachable_objects(repo, list(haves) + list(client_shallow),
                            shallow_boundary=set(client_shallow))
    include = reachable_objects(repo, wants, shallow_boundary=set(boundary))
    pack = {sha: obj for sha, obj in include.items() if sha not in had}
    return pack


def _shallow_boundary(repo, wants, depth):
    """Commits exactly `depth` steps below the wants (their parents stay home)."""
    if depth is None or depth <= 0:
        return set()
    level = {w: 1 for w in wants}
    frontier = list(wants)
    d = 1
    while d < depth and frontier:
        nxt = []
        for sha in frontier:
            obj = get_object(repo, sha)
            if obj and obj[0] == 'commit':
                for p in parse_commit(obj[1])['parents']:
                    if p not in level:
                        level[p] = d + 1
                        nxt.append(p)
        frontier = nxt
        d += 1
    return {sha for sha, lv in level.items() if lv == depth}


def _best_common(repo, wants, haves):
    want_anc = set()
    for w in wants:
        want_anc |= ancestors(repo, w)
    for h in haves:
        if h in want_anc:
            return h
    return None


def _parse_fetch_args(lines):
    args = {'want': [], 'have': [], 'shallow': [], 'deepen': None, 'done': False}
    for raw in lines:
        line = raw.decode('utf-8', 'replace').rstrip('\n')
        if line.startswith('want '):
            args['want'].append(line[5:].split(' ')[0].strip())
        elif line.startswith('have '):
            args['have'].append(line[5:].strip())
        elif line.startswith('shallow '):
            args['shallow'].append(line[8:].strip())
        elif line.startswith('deepen '):
            try:
                args['deepen'] = int(line[7:].strip())
            except ValueError:
                pass
        elif line == 'done':
            args['done'] = True
    return args


def _upload_pack_v2(ctx, repo, packets):
    # capability section up to DELIM, then argument lines up to FLUSH
    idx = 0
    command = None
    arg_lines = []
    caps_done = False
    for payload, _pos in packets:
        if payload is None:
            break
        if payload == 'delim':
            caps_done = True
            continue
        if not caps_done:
            text = payload.decode('utf-8', 'replace').rstrip('\n')
            if text.startswith('command='):
                command = text[len('command='):]
        else:
            arg_lines.append(payload)
    if command == 'ls-refs':
        return _ls_refs(ctx, repo, arg_lines)
    if command == 'fetch':
        return _fetch_v2(ctx, repo, arg_lines)
    return ctx.send_raw(200, FLUSH, 'application/x-git-upload-pack-result')


def _ls_refs(ctx, repo, arg_lines):
    peel = symrefs = unborn = False
    prefixes = []
    for raw in arg_lines:
        line = raw.decode('utf-8', 'replace').rstrip('\n')
        if line == 'peel':
            peel = True
        elif line == 'symrefs':
            symrefs = True
        elif line == 'unborn':
            unborn = True
        elif line.startswith('ref-prefix '):
            prefixes.append(line[len('ref-prefix '):])

    def wanted(name):
        return not prefixes or any(name.startswith(p) for p in prefixes)

    out = bytearray()
    head_sha = repo.refs.get(repo.head)
    if wanted('HEAD'):
        if head_sha is not None:
            line = f'{head_sha} HEAD'
            if symrefs:
                line += f' symref-target:{repo.head}'
            out += pkt(line + '\n')
        elif unborn:
            line = 'unborn HEAD'
            if symrefs:
                line += f' symref-target:{repo.head}'
            out += pkt(line + '\n')
    for ref in sorted(repo.refs):
        if not wanted(ref):
            continue
        sha = repo.refs[ref]
        line = f'{sha} {ref}'
        if peel and _is_tag_obj(repo, sha):
            peeled = _peel(repo, sha)
            if peeled:
                line += f' peeled:{peeled}'
        out += pkt(line + '\n')
    out += FLUSH
    ctx.send_raw(200, bytes(out), 'application/x-git-upload-pack-result')


def _fetch_v2(ctx, repo, arg_lines):
    args = _parse_fetch_args(arg_lines)
    wants = args['want']
    haves = args['have']
    client_shallow = args['shallow']
    boundary = _shallow_boundary(repo, wants, args['deepen'])
    with _store_lock:
        pack_objs = _fetch_objects(repo, wants, haves, client_shallow, boundary)
    common = _best_common(repo, wants, haves)
    out = bytearray()
    if haves:
        out += pkt('acknowledgments\n')
        if common:
            out += pkt(f'ACK {common}\n')
        else:
            out += pkt('NAK\n')
        if pack_objs:
            out += pkt('ready\n')
        out += FLUSH
    if args['deepen'] is not None or client_shallow:
        lines = [f'shallow {s}\n' for s in sorted(boundary)]
        lines += [f'unshallow {s}\n' for s in sorted(client_shallow)
                  if s not in boundary and s in repo.objects]
        if lines:
            out += pkt('shallow-info\n')
            for line in lines:
                out += pkt(line)
            out += FLUSH
    if pack_objs:
        pack = write_pack([(sha, t, d) for sha, (t, d) in pack_objs.items()])
        out += pkt('packfile\n')
        out += _sideband_packets(pack)
        out += FLUSH
    if not out:
        out += FLUSH
    ctx.send_raw(200, bytes(out), 'application/x-git-upload-pack-result')


def _all_pkt_payloads(body):
    """Every data payload in a pkt-line stream, across all flush boundaries."""
    out = []
    pos = 0
    while pos + 4 <= len(body):
        try:
            n = int(body[pos:pos + 4], 16)
        except ValueError:
            break
        if n == 0:
            pos += 4
            continue
        if n in (1, 2):
            pos += 4
            continue
        out.append(body[pos + 4:pos + n])
        pos += n
    return out


def _upload_pack_v0(ctx, repo, body):
    lines = _all_pkt_payloads(body)
    args = _parse_fetch_args(lines)
    wants = args['want']
    haves = args['have']
    client_shallow = args['shallow']
    boundary = _shallow_boundary(repo, wants, args['deepen'])
    with _store_lock:
        pack_objs = _fetch_objects(repo, wants, haves, client_shallow, boundary)
    common = _best_common(repo, wants, haves)
    out = bytearray()
    if args['deepen'] is not None or client_shallow:
        for s in sorted(boundary):
            out += pkt(f'shallow {s}\n')
        for s in sorted(client_shallow):
            if s not in boundary and s in repo.objects:
                out += pkt(f'unshallow {s}\n')
    if pack_objs:
        if common:
            out += pkt(f'ACK {common}\n')
        else:
            out += pkt('NAK\n')
        pack = write_pack([(sha, t, d) for sha, (t, d) in pack_objs.items()])
        out += _sideband_packets(pack)
    else:
        if common:
            out += pkt(f'ACK {common}\n')
        else:
            out += pkt('NAK\n')
    out += FLUSH
    ctx.send_raw(200, bytes(out), 'application/x-git-upload-pack-result')


def _receive_pack(ctx, repo, body):
    packets, rest = read_pkt_stream(body)
    commands = []
    client_caps = set()
    for payload in packets:
        if payload is None:
            break
        if not isinstance(payload, bytes):
            continue
        line = payload.decode('utf-8', 'replace').rstrip('\n')
        if '\0' in line:
            line, caps = line.split('\0', 1)
            client_caps = set(caps.split())
        parts = line.split(' ')
        if len(parts) == 3 and len(parts[0]) == 40 and len(parts[1]) == 40:
            commands.append((parts[0], parts[1], parts[2]))
    pack = rest
    with _store_lock:
        unpack_ok = True
        unpack_msg = 'ok'
        if pack:
            try:
                read_pack(repo, pack)
            except Exception as exc:
                unpack_ok = False
                unpack_msg = f'error: {exc}'
        results = []
        if not unpack_ok:
            results = [(ref, False, 'unpack failed') for _o, _n, ref in commands]
        else:
            staged = []
            for old, new, ref in commands:
                current = repo.refs.get(ref, ZERO_SHA)
                ok, reason = True, ''
                if new == ZERO_SHA:  # delete
                    if ref == repo.head:
                        ok, reason = False, 'deletion of the current branch prohibited'
                    elif current == ZERO_SHA:
                        ok, reason = False, 'reference does not exist'
                    elif old != ZERO_SHA and old != current:
                        ok, reason = False, 'stale info'
                else:
                    if get_object(repo, new) is None:
                        ok, reason = False, 'missing object'
                    elif old == ZERO_SHA and current != ZERO_SHA:
                        ok, reason = False, 'reference already exists'
                    elif old != ZERO_SHA and current == ZERO_SHA:
                        ok, reason = False, 'reference does not exist'
                    elif old != current:
                        ok, reason = False, 'stale info'
                    elif current != ZERO_SHA and not is_ancestor(repo, current, new):
                        ok, reason = False, 'non-fast-forward'
                results.append((ref, ok, reason))
                if ok:
                    staged.append((ref, new))
            for ref, new in staged:
                if new == ZERO_SHA:
                    repo.refs.pop(ref, None)
                else:
                    repo.refs[ref] = new
            if staged:
                repo.pushed_at = tick()
                repo.updated_at = repo.pushed_at
        report = bytearray()
        report += pkt(f'unpack {unpack_msg}\n')
        for ref, ok, reason in results:
            report += pkt(f'ok {ref}\n' if ok else f'ng {ref} {reason}\n')
        report += FLUSH
        if 'side-band-64k' in client_caps:
            out = bytearray()
            pos = 0
            raw = bytes(report)
            while pos < len(raw):
                # wrap each pkt-line of the report in a band-1 packet
                try:
                    n = int(raw[pos:pos + 4], 16)
                except ValueError:
                    break
                if n == 0:
                    break
                out += pkt(b'\x01' + raw[pos + 4:pos + n])
                pos += n
            out += FLUSH
            return ctx.send_raw(200, bytes(out), 'application/x-git-receive-pack-result')
        return ctx.send_raw(200, bytes(report), 'application/x-git-receive-pack-result')


def handle_git(ctx, owner, repo_name, tail):
    repo = STORE.get(owner, repo_name)
    if tail == 'info/refs':
        if repo is None:
            return ctx.error(404, 'Not Found')
        service = ctx.q('service') or ''
        if service not in ('git-upload-pack', 'git-receive-pack'):
            return ctx.error(400, 'Bad service')
        if service == 'git-upload-pack' and ctx.headers.get('Git-Protocol') == 'version=2':
            return ctx.send_raw(200, _advertise_v2(), f'application/x-{service}-advertisement')
        return ctx.send_raw(200, _advertise_v0(repo, service), f'application/x-{service}-advertisement')
    if repo is None:
        return ctx.error(404, 'Not Found')
    if tail == 'git-upload-pack' and ctx.method == 'POST':
        if ctx.headers.get('Git-Protocol') == 'version=2':
            packets = list(parse_pkt_lines(ctx.body))
            return _upload_pack_v2(ctx, repo, packets)
        return _upload_pack_v0(ctx, repo, ctx.body)
    if tail == 'git-receive-pack' and ctx.method == 'POST':
        return _receive_pack(ctx, repo, ctx.body)
    return ctx.error(404, 'Not Found')


def _branch_tip(repo, branch):
    return repo.refs.get(f'refs/heads/{branch}')


def _pr_head_ref(body_ref):
    # GitHub accepts "branch" or "owner:branch"
    return body_ref.split(':', 1)[-1] if body_ref else body_ref


def pull_json(repo, issue):
    pull = issue['pull']
    head_ref = pull['head_ref']
    base_ref = pull['base_ref']
    head_sha = _branch_tip(repo, head_ref) or pull['head_sha']
    base_sha = _branch_tip(repo, base_ref) or pull['base_sha']
    out = issue_json(repo, issue)
    out.update({
        'url': f"/api/v3/repos/{repo.full_name}/pulls/{issue['number']}",
        'html_url': f"/{repo.full_name}/pull/{issue['number']}",
        'diff_url': f"/{repo.full_name}/pull/{issue['number']}.diff",
        'patch_url': f"/{repo.full_name}/pull/{issue['number']}.patch",
        'merged_at': iso(pull['merged_at']) if pull.get('merged_at') else None,
        'merge_commit_sha': pull.get('merge_commit_sha'),
        'merged': bool(pull.get('merged')),
        'mergeable': pull.get('mergeable'),
        'mergeable_state': 'clean' if pull.get('mergeable') else 'dirty',
        'comments': len(issue.get('comments', [])),
        'review_comments': 0,
        'commits': 0,
        'additions': 0,
        'deletions': 0,
        'changed_files': 0,
        'head': {
            'label': f"{repo.owner}:{head_ref}",
            'ref': head_ref,
            'sha': head_sha,
            'user': user_json(USERS_BY_LOGIN.get(repo.owner) or VIEWER),
            'repo': repo_json(repo),
        },
        'base': {
            'label': f"{repo.owner}:{base_ref}",
            'ref': base_ref,
            'sha': base_sha,
            'user': user_json(USERS_BY_LOGIN.get(repo.owner) or VIEWER),
            'repo': repo_json(repo),
        },
        '_links': {},
    })
    if out.get('body') is None:
        out['body'] = ''
    return out


def _merge_analysis(repo, head_sha, base_sha):
    """(merge_base_sha, merged flat map or None, conflicts list)."""
    mb = merge_base(repo, base_sha, head_sha)
    if mb is None:
        return None, None, ['no common ancestor']
    base_flat = flatten_tree(repo, commit_tree_sha(repo, mb))
    ours = flatten_tree(repo, commit_tree_sha(repo, base_sha))
    theirs = flatten_tree(repo, commit_tree_sha(repo, head_sha))
    merged = {}
    conflicts = []
    for path in set(base_flat) | set(ours) | set(theirs):
        b = base_flat.get(path)
        o = ours.get(path)
        t = theirs.get(path)
        if o == t:
            if o is not None:
                merged[path] = o
        elif b == o:
            if t is not None:
                merged[path] = t
        elif b == t:
            if o is not None:
                merged[path] = o
        else:
            conflicts.append(path)
    return mb, (None if conflicts else merged), conflicts


def _pr_mergeable(repo, pull):
    if pull.get('merged'):
        return True
    head_sha = _branch_tip(repo, pull['head_ref'])
    base_sha = _branch_tip(repo, pull['base_ref'])
    if head_sha is None or base_sha is None:
        return None
    _mb, merged, _c = _merge_analysis(repo, head_sha, base_sha)
    return merged is not None


def pulls_create(ctx, owner, name):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        title = body.get('title')
        head_ref = _pr_head_ref(body.get('head') or '')
        base_ref = _pr_head_ref(body.get('base') or repo.default_branch)
        head_sha = _branch_tip(repo, head_ref)
        base_sha = _branch_tip(repo, base_ref)
        if title is None or head_sha is None or base_sha is None:
            return ctx.error(422, 'Validation Failed', {'errors': [
                {'resource': 'PullRequest', 'field': 'head' if head_sha is None else 'base',
                 'code': 'invalid'}]})
        pull = {'head_ref': head_ref, 'base_ref': base_ref,
                'head_sha': head_sha, 'base_sha': base_sha,
                'merged': False, 'merged_at': None, 'merge_commit_sha': None,
                'mergeable': None}
        issue = _new_issue(repo, title, body.get('body') or '', body.get('labels') or [],
                           is_pull=True, pull=pull)
        pull['mergeable'] = _pr_mergeable(repo, pull)
        ctx.send(201, pull_json(repo, issue))


def pulls_list(ctx, owner, name):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    state = ctx.q('state', 'open')
    items = [i for i in repo.issues.values() if i.get('pull_request')]
    if state == 'open':
        items = [i for i in items if i['state'] == 'open']
    elif state == 'closed':
        items = [i for i in items if i['state'] == 'closed']
    direction = ctx.q('direction', 'desc')
    items.sort(key=lambda i: i['created_at'], reverse=(direction != 'asc'))
    out = [pull_json(repo, i) for i in items]
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def _get_pull(repo, number):
    issue = repo.issues.get(int(number))
    if issue is None or not issue.get('pull_request'):
        return None
    return issue


def pulls_get(ctx, owner, name, number):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    issue = _get_pull(repo, number)
    if issue is None:
        return ctx.error(404, 'Not Found')
    issue['pull']['mergeable'] = _pr_mergeable(repo, issue['pull'])
    ctx.send(200, pull_json(repo, issue))


def pulls_patch(ctx, owner, name, number):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        issue = _get_pull(repo, number)
        if issue is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        if 'title' in body:
            issue['title'] = body['title']
        if 'body' in body:
            issue['body'] = body['body']
        if 'state' in body:
            issue['state'] = body['state']
            issue['closed_at'] = tick() if body['state'] == 'closed' else None
        issue['updated_at'] = tick()
        ctx.send(200, pull_json(repo, issue))


def _diff_counts(repo, old, new):
    """(additions, deletions) between two blob shas, line based."""
    import difflib
    old_lines = []
    new_lines = []
    if old is not None:
        obj = get_object(repo, old)
        if obj:
            old_lines = obj[1].decode('utf-8', 'replace').splitlines()
    if new is not None:
        obj = get_object(repo, new)
        if obj:
            new_lines = obj[1].decode('utf-8', 'replace').splitlines()
    add = dele = 0
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, old_lines, new_lines).get_opcodes():
        if tag in ('replace', 'delete'):
            dele += i2 - i1
        if tag in ('replace', 'insert'):
            add += j2 - j1
    return add, dele


def pulls_files(ctx, owner, name, number):
    repo = STORE.get(owner, name)
    if repo is None:
        return ctx.error(404, 'Not Found')
    issue = _get_pull(repo, number)
    if issue is None:
        return ctx.error(404, 'Not Found')
    pull = issue['pull']
    head_sha = _branch_tip(repo, pull['head_ref']) or pull['head_sha']
    base_sha = _branch_tip(repo, pull['base_ref']) or pull['base_sha']
    mb = merge_base(repo, base_sha, head_sha) or base_sha
    base_flat = flatten_tree(repo, commit_tree_sha(repo, mb))
    head_flat = flatten_tree(repo, commit_tree_sha(repo, head_sha))
    out = []
    for path in sorted(set(base_flat) | set(head_flat)):
        b = base_flat.get(path)
        h = head_flat.get(path)
        if b == h:
            continue
        if b is None:
            status = 'added'
        elif h is None:
            status = 'removed'
        else:
            status = 'modified'
        add, dele = _diff_counts(repo, b[1] if b else None, h[1] if h else None)
        out.append({
            'sha': h[1] if h else b[1],
            'filename': path,
            'status': status,
            'additions': add,
            'deletions': dele,
            'changes': add + dele,
            'blob_url': f"/api/v3/repos/{repo.full_name}/git/blobs/{h[1] if h else b[1]}",
            'raw_url': f"/raw/{repo.full_name}/{head_sha}/{path}",
            'contents_url': f"/api/v3/repos/{repo.full_name}/contents/{path}?ref={pull['head_ref']}",
        })
    sliced, headers = paginate(ctx, out)
    ctx.send(200, sliced, headers=headers)


def _merge_pull(repo, issue, method='merge', commit_title=None, commit_message=None):
    """Merge a pull request. Returns (merge_sha, None) or (None, 'reason')."""
    pull = issue['pull']
    if pull.get('merged'):
        return None, 'already merged'
    head_sha = _branch_tip(repo, pull['head_ref'])
    base_sha = _branch_tip(repo, pull['base_ref'])
    if head_sha is None or base_sha is None:
        return None, 'missing branch'
    _mb, merged_flat, _conflicts = _merge_analysis(repo, head_sha, base_sha)
    if merged_flat is None:
        return None, 'conflict'
    tree = build_tree_from_flat(repo, merged_flat)
    now = tick()
    ident = f"{VIEWER['name']} <{VIEWER['email']}> {now} +0000"
    if method == 'squash':
        message = commit_title or f"{issue['title']} (#{issue['number']})"
        parents = [base_sha]
    else:
        message = commit_title or f"Merge pull request #{issue['number']} from {pull['head_ref']}"
        parents = [base_sha, head_sha]
    if commit_message:
        message = f'{message}\n\n{commit_message}'
    commit_sha = store_object(repo, 'commit',
                              build_commit(tree, parents, ident, ident, message))
    repo.refs[f"refs/heads/{pull['base_ref']}"] = commit_sha
    repo.pushed_at = now
    pull['merged'] = True
    pull['merged_at'] = now
    pull['merge_commit_sha'] = commit_sha
    pull['mergeable'] = True
    issue['state'] = 'closed'
    issue['closed_at'] = now
    issue['updated_at'] = now
    return commit_sha, None


def pulls_merge(ctx, owner, name, number):
    with _store_lock:
        repo = STORE.get(owner, name)
        if repo is None:
            return ctx.error(404, 'Not Found')
        issue = _get_pull(repo, number)
        if issue is None:
            return ctx.error(404, 'Not Found')
        body = ctx.json or {}
        sha, err = _merge_pull(repo, issue, body.get('merge_method') or 'merge',
                               body.get('commit_title'), body.get('commit_message'))
        if sha is None:
            return ctx.error(405, 'Pull Request is not mergeable')
        ctx.send(200, {'sha': sha, 'merged': True,
                       'message': 'Pull Request successfully merged'})


# --------------------------------------------------------------------------
# GraphQL v4 (/api/graphql): a small purpose-built parser + executor over
# the same in-memory store the REST surface uses. Supports the document
# shapes gh emits: operations with variable definitions, aliases, inline
# fragments, input objects, enums, lists, batching and __type introspection.
# --------------------------------------------------------------------------

_GQL_PUNCT = set('!$():=@[]{}|&')


def _gql_tokens(text):
    toks = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c in ' \t\r\n,':
            i += 1
        elif c == '#':
            while i < n and text[i] != '\n':
                i += 1
        elif text.startswith('...', i):
            toks.append(('...', '...'))
            i += 3
        elif c == '"':
            if text.startswith('"""', i):
                j = text.find('"""', i + 3)
                if j < 0:
                    raise ValueError('unterminated block string')
                toks.append(('str', text[i + 3:j]))
                i = j + 3
            else:
                j = i + 1
                buf = []
                while j < n and text[j] != '"':
                    if text[j] == '\\':
                        esc = text[j + 1]
                        mapping = {'n': '\n', 't': '\t', 'r': '\r', 'b': '\b',
                                   'f': '\f', '/': '/', '\\': '\\', '"': '"'}
                        if esc == 'u':
                            buf.append(chr(int(text[j + 2:j + 6], 16)))
                            j += 6
                        else:
                            buf.append(mapping.get(esc, esc))
                            j += 2
                    else:
                        buf.append(text[j])
                        j += 1
                if j >= n:
                    raise ValueError('unterminated string')
                toks.append(('str', ''.join(buf)))
                i = j + 1
        elif c.isalpha() or c == '_':
            j = i
            while j < n and (text[j].isalnum() or text[j] == '_'):
                j += 1
            toks.append(('name', text[i:j]))
            i = j
        elif c.isdigit() or (c == '-' and i + 1 < n and text[i + 1].isdigit()):
            j = i
            if text[j] == '-':
                j += 1
            while j < n and text[j].isdigit():
                j += 1
            is_float = False
            if j < n and text[j] == '.':
                is_float = True
                j += 1
                while j < n and text[j].isdigit():
                    j += 1
            if j < n and text[j] in 'eE':
                is_float = True
                j += 1
                if j < n and text[j] in '+-':
                    j += 1
                while j < n and text[j].isdigit():
                    j += 1
            toks.append(('float' if is_float else 'int', text[i:j]))
            i = j
        elif c in _GQL_PUNCT:
            toks.append((c, c))
            i += 1
        else:
            raise ValueError(f'unexpected character {c!r}')
    toks.append(('eof', ''))
    return toks


class _GqlParser:
    def __init__(self, text):
        self.toks = _gql_tokens(text)
        self.pos = 0

    def peek(self):
        return self.toks[self.pos]

    def next(self):
        tok = self.toks[self.pos]
        self.pos += 1
        return tok

    def expect(self, kind):
        tok = self.next()
        if tok[0] != kind:
            raise ValueError(f'expected {kind}, got {tok[1]!r}')
        return tok[1]

    def parse_document(self):
        ops = []
        while self.peek()[0] != 'eof':
            ops.append(self.parse_operation())
        return ops

    def parse_operation(self):
        kind, val = self.peek()
        if kind == '{':
            return {'type': 'query', 'name': None, 'var_defs': {},
                    'selection': self.parse_selection_set()}
        if kind == 'name' and val in ('query', 'mutation', 'subscription'):
            self.next()
            name = None
            if self.peek()[0] == 'name':
                name = self.next()[1]
            var_defs = {}
            if self.peek()[0] == '(':
                var_defs = self.parse_var_defs()
            self.skip_directives()
            return {'type': val, 'name': name, 'var_defs': var_defs,
                    'selection': self.parse_selection_set()}
        if kind == 'name' and val == 'fragment':
            self.next()
            fname = self.expect('name')
            self.expect('name')  # 'on'
            tname = self.expect('name')
            self.skip_directives()
            return {'type': 'fragment', 'name': fname, 'on': tname,
                    'selection': self.parse_selection_set()}
        raise ValueError(f'unexpected token {val!r}')

    def parse_var_defs(self):
        defs = {}
        self.expect('(')
        while self.peek()[0] != ')':
            self.expect('$')
            name = self.expect('name')
            self.expect(':')
            self.parse_type()
            default = None
            if self.peek()[0] == '=':
                self.next()
                default = self.parse_value()
            defs[name] = default
        self.expect(')')
        return defs

    def parse_type(self):
        if self.peek()[0] == '[':
            self.next()
            self.parse_type()
            self.expect(']')
        else:
            self.expect('name')
        if self.peek()[0] == '!':
            self.next()

    def skip_directives(self):
        while self.peek()[0] == '@':
            self.next()
            self.expect('name')
            if self.peek()[0] == '(':
                self.parse_arguments()

    def parse_selection_set(self):
        self.expect('{')
        sels = []
        while self.peek()[0] != '}':
            sels.append(self.parse_selection())
        self.expect('}')
        return sels

    def parse_selection(self):
        if self.peek()[0] == '...':
            self.next()
            if self.peek()[0] == 'name' and self.peek()[1] == 'on':
                self.next()
                tname = self.expect('name')
                self.skip_directives()
                return {'kind': 'inline', 'type': tname,
                        'selection': self.parse_selection_set()}
            fname = self.expect('name')
            return {'kind': 'spread', 'name': fname}
        name = self.expect('name')
        alias = None
        if self.peek()[0] == ':':
            self.next()
            alias = name
            name = self.expect('name')
        args = {}
        if self.peek()[0] == '(':
            args = self.parse_arguments()
        self.skip_directives()
        sel = None
        if self.peek()[0] == '{':
            sel = self.parse_selection_set()
        return {'kind': 'field', 'alias': alias, 'name': name,
                'args': args, 'selection': sel}

    def parse_arguments(self):
        args = {}
        self.expect('(')
        while self.peek()[0] != ')':
            key = self.expect('name')
            self.expect(':')
            args[key] = self.parse_value()
        self.expect(')')
        return args

    def parse_value(self):
        kind, val = self.next()
        if kind == '$':
            return ('var', self.expect('name'))
        if kind == 'str':
            return ('lit', val)
        if kind == 'int':
            return ('lit', int(val))
        if kind == 'float':
            return ('lit', float(val))
        if kind == 'name':
            if val == 'true':
                return ('lit', True)
            if val == 'false':
                return ('lit', False)
            if val == 'null':
                return ('lit', None)
            return ('enum', val)
        if kind == '[':
            items = []
            while self.peek()[0] != ']':
                items.append(self.parse_value())
            self.expect(']')
            return ('list', items)
        if kind == '{':
            obj = {}
            while self.peek()[0] != '}':
                key = self.expect('name')
                self.expect(':')
                obj[key] = self.parse_value()
            self.expect('}')
            return ('obj', obj)
        raise ValueError(f'unexpected value token {val!r}')


def _gql_value(node, variables):
    kind = node[0]
    if kind == 'lit':
        return node[1]
    if kind == 'enum':
        return node[1]
    if kind == 'var':
        return variables.get(node[1])
    if kind == 'list':
        return [_gql_value(v, variables) for v in node[1]]
    if kind == 'obj':
        return {k: _gql_value(v, variables) for k, v in node[1].items()}
    return None


# --- global id helpers -----------------------------------------------------

def _decode_global_id(gid):
    try:
        raw = base64.b64decode(str(gid).encode()).decode()
        kind, _, raw_id = raw.partition(':')
        return kind, raw_id
    except Exception:
        return None, None


def _find_issue_by_raw_id(raw_id):
    try:
        gid = int(raw_id)
    except (TypeError, ValueError):
        return None, None
    for repo in STORE.repos.values():
        for issue in repo.issues.values():
            if issue['id'] == gid:
                return repo, issue
    return None, None


def _repo_from_global_id(gid):
    kind, raw_id = _decode_global_id(gid)
    if kind == 'Repository':
        try:
            return STORE.by_id(int(raw_id))
        except (TypeError, ValueError):
            return None
    try:
        return STORE.by_id(int(gid))
    except (TypeError, ValueError):
        return None


def _gql_node(gid):
    kind, raw_id = _decode_global_id(gid)
    if kind == 'Repository':
        repo = _repo_from_global_id(gid)
        return ('Repository', repo) if repo else None
    if kind in ('Issue', 'PullRequest'):
        repo, issue = _find_issue_by_raw_id(raw_id)
        if issue is None:
            return None
        if issue.get('pull_request'):
            return ('PullRequest', (repo, issue))
        return ('Issue', (repo, issue))
    if kind == 'User':
        try:
            uid = int(raw_id)
        except (TypeError, ValueError):
            return None
        for u in USERS:
            if u['id'] == uid:
                return ('User', u)
        return None
    if kind == 'IssueComment':
        try:
            cid = int(raw_id)
        except (TypeError, ValueError):
            return None
        for repo in STORE.repos.values():
            for issue in repo.issues.values():
                for comment in issue['comments']:
                    if comment['id'] == cid:
                        return ('IssueComment', (repo, issue, comment))
        return None
    return None


# --- connections -----------------------------------------------------------

def _gql_cursor(index):
    return base64.b64encode(f'cursor:{index}'.encode()).decode()


def _gql_cursor_index(cursor):
    try:
        raw = base64.b64decode(str(cursor).encode()).decode()
        if raw.startswith('cursor:'):
            return int(raw[len('cursor:'):])
    except Exception:
        pass
    return -1


def _gql_connection(conn_type, edge_type, tagged, args):
    """Slice a full ordered list of tagged nodes into a Relay connection."""
    start = 0
    after = args.get('after')
    if after is not None:
        idx = _gql_cursor_index(after)
        if idx >= 0:
            start = idx + 1
    first = args.get('first')
    if first is None:
        first = 30
    page = tagged[start:start + first]
    indices = list(range(start, start + len(page)))
    page_info = {
        'hasNextPage': start + len(page) < len(tagged),
        'hasPreviousPage': start > 0,
        'startCursor': _gql_cursor(indices[0]) if indices else None,
        'endCursor': _gql_cursor(indices[-1]) if indices else None,
    }
    edges = [(edge_type, {'node': item, 'cursor': _gql_cursor(idx)})
             for item, idx in zip(page, indices)]
    return (conn_type, {'nodes': page, 'edges': edges,
                        'pageInfo': ('PageInfo', page_info),
                        'totalCount': len(tagged)})


def _gql_order(items, order_by, default_field='CREATED_AT'):
    order = order_by or {}
    field = order.get('field') or default_field
    direction = order.get('direction') or 'DESC'
    if field == 'UPDATED_AT':
        keyf = lambda i: i['updated_at']
    elif field == 'NUMBER':
        keyf = lambda i: i['number']
    else:
        keyf = lambda i: i['created_at']
    items.sort(key=keyf, reverse=(direction != 'ASC'))
    return items


def _gql_pr_state(issue):
    if issue['pull'].get('merged'):
        return 'MERGED'
    return 'OPEN' if issue['state'] == 'open' else 'CLOSED'


def _gql_pr_files(repo, issue):
    pull = issue['pull']
    head_sha = _branch_tip(repo, pull['head_ref']) or pull['head_sha']
    base_sha = _branch_tip(repo, pull['base_ref']) or pull['base_sha']
    mb = merge_base(repo, base_sha, head_sha) or base_sha
    base_flat = flatten_tree(repo, commit_tree_sha(repo, mb))
    head_flat = flatten_tree(repo, commit_tree_sha(repo, head_sha))
    out = []
    for path in sorted(set(base_flat) | set(head_flat)):
        b = base_flat.get(path)
        h = head_flat.get(path)
        if b == h:
            continue
        add, dele = _diff_counts(repo, b[1] if b else None, h[1] if h else None)
        out.append(('PullRequestChangedFile',
                    {'path': path, 'additions': add, 'deletions': dele}))
    return out


# --- schema (introspection surface) ----------------------------------------

_GQL_TYPE_FIELDS = {
    'Query': ['viewer', 'repository', 'node', 'user', 'organization',
              'search', 'rateLimit', 'meta'],
    'Mutation': ['createIssue', 'closeIssue', 'reopenIssue', 'updateIssue',
                 'createPullRequest', 'mergePullRequest', 'addComment',
                 'addLabelsToLabelable', 'removeLabelsFromLabelable'],
    'User': ['id', 'login', 'name', 'email', 'url', 'avatarUrl', 'bio',
             'company', 'location', 'createdAt', 'updatedAt', 'isViewer',
             'databaseId'],
    'Repository': ['id', 'databaseId', 'name', 'nameWithOwner', 'owner',
                   'description', 'isPrivate', 'private', 'isFork',
                   'defaultBranchRef', 'viewerPermission', 'hasIssuesEnabled',
                   'hasPullRequestsEnabled', 'issue', 'issues', 'pullRequest',
                   'pullRequests', 'issueOrPullRequest', 'labels', 'refs',
                   'createdAt', 'updatedAt', 'pushedAt', 'url', 'homepageUrl',
                   'stargazerCount', 'forkCount', 'primaryLanguage'],
    'Issue': ['id', 'databaseId', 'number', 'title', 'state', 'stateReason',
              'body', 'bodyText', 'author', 'repository', 'labels',
              'comments', 'assignees', 'milestone', 'createdAt', 'updatedAt',
              'closedAt', 'closed', 'locked', 'url', 'viewerDidAuthor'],
    'PullRequest': ['id', 'databaseId', 'number', 'title', 'state', 'body',
                    'bodyText', 'author', 'repository', 'headRefName',
                    'baseRefName', 'headRef', 'baseRef', 'mergeable',
                    'merged', 'mergedAt', 'closed', 'closedAt', 'files',
                    'commits', 'comments', 'additions', 'deletions',
                    'changedFiles', 'mergeCommit', 'createdAt', 'updatedAt',
                    'url', 'isDraft'],
    'Ref': ['name', 'prefix', 'target', 'repository'],
    'Commit': ['oid', 'abbreviatedOid', 'message', 'committedDate', 'author'],
    'Label': ['id', 'name', 'color', 'description', 'url'],
    'IssueComment': ['id', 'databaseId', 'body', 'bodyText', 'author',
                     'createdAt', 'updatedAt', 'url'],
    'PageInfo': ['hasNextPage', 'hasPreviousPage', 'startCursor', 'endCursor'],
    'IssueConnection': ['nodes', 'edges', 'pageInfo', 'totalCount'],
    'PullRequestConnection': ['nodes', 'edges', 'pageInfo', 'totalCount'],
    'PullRequestChangedFileConnection': ['nodes', 'edges', 'pageInfo',
                                         'totalCount'],
    'PullRequestChangedFile': ['path', 'additions', 'deletions'],
    'LabelConnection': ['nodes', 'edges', 'pageInfo', 'totalCount'],
    'IssueCommentConnection': ['nodes', 'edges', 'pageInfo', 'totalCount'],
}

_GQL_TYPE_ENUMS = {
    'SearchType': ['ISSUE', 'REPOSITORY', 'USER', 'DISCUSSION'],
    'IssueState': ['OPEN', 'CLOSED'],
    'PullRequestState': ['OPEN', 'CLOSED', 'MERGED'],
    'MergeableState': ['MERGEABLE', 'CONFLICTING', 'UNKNOWN'],
    'PullRequestMergeMethod': ['MERGE', 'SQUASH', 'REBASE'],
    'RepositoryPermission': ['ADMIN', 'MAINTAIN', 'WRITE', 'TRIAGE', 'READ'],
    'OrderDirection': ['ASC', 'DESC'],
    'IssueOrderField': ['CREATED_AT', 'UPDATED_AT', 'COMMENTS'],
    'PullRequestOrderField': ['CREATED_AT', 'UPDATED_AT'],
    'StateReason': ['COMPLETED', 'NOT_PLANNED', 'REOPENED'],
}

_GQL_SCALARS = {'String', 'Int', 'Float', 'Boolean', 'ID', 'URI', 'DateTime',
                'GitObjectID', 'HTML', 'Base64String'}


def _gql_type(name):
    if name in _GQL_TYPE_FIELDS:
        return ('__Type', {'name': name, 'kind': 'OBJECT'})
    if name in _GQL_TYPE_ENUMS:
        return ('__Type', {'name': name, 'kind': 'ENUM'})
    if name in _GQL_SCALARS:
        return ('__Type', {'name': name, 'kind': 'SCALAR'})
    if name in ('Node', 'Actor', 'RepositoryOwner'):
        return ('__Type', {'name': name, 'kind': 'INTERFACE'})
    return None


# --- field resolution --------------------------------------------------------

_MISSING = object()


def _gql_user_obj(login):
    return USERS_BY_LOGIN.get(login) or VIEWER


def _gql_resolve(type_name, obj, field, args, gtx):
    if type_name == 'Query':
        if field == 'viewer':
            return ('User', VIEWER)
        if field == 'repository':
            repo = STORE.get(args.get('owner') or '', args.get('name') or '')
            return ('Repository', repo) if repo else None
        if field == 'node':
            return _gql_node(args.get('id'))
        if field == 'user':
            u = USERS_BY_LOGIN.get(args.get('login') or '')
            return ('User', u) if u else None
        if field == 'organization':
            return None
        if field == '__type':
            return _gql_type(args.get('name'))
        if field == '__schema':
            return ('__Schema', {})
        return _MISSING

    if type_name == 'Mutation':
        return _gql_mutation(field, args, gtx)

    if type_name == '__Type':
        if field == 'name':
            return obj['name']
        if field == 'kind':
            return obj['kind']
        if field == 'description':
            return None
        if field == 'fields':
            names = _GQL_TYPE_FIELDS.get(obj['name'])
            if names is None:
                return None
            return [('__Field', {'name': n}) for n in names]
        if field == 'enumValues':
            values = _GQL_TYPE_ENUMS.get(obj['name'])
            if values is None:
                return None
            return [('__EnumValue', {'name': v}) for v in values]
        if field == 'possibleTypes':
            if obj['name'] == 'Node':
                return [_gql_type(t) for t in
                        ('Repository', 'Issue', 'PullRequest', 'User',
                         'IssueComment')]
            return None
        if field == 'inputFields':
            return []
        if field == 'interfaces':
            return []
        if field == 'specifiedByURL':
            return None
        return _MISSING

    if type_name == '__Field':
        if field == 'name':
            return obj['name']
        if field == 'description':
            return None
        if field == 'isDeprecated':
            return False
        if field == 'deprecationReason':
            return None
        if field == 'args':
            return []
        if field == 'type':
            return ('__Type', {'name': 'String', 'kind': 'SCALAR'})
        return _MISSING

    if type_name == '__EnumValue':
        if field == 'name':
            return obj['name']
        if field == 'description':
            return None
        if field == 'isDeprecated':
            return False
        if field == 'deprecationReason':
            return None
        return _MISSING

    if type_name == '__Schema':
        if field == 'queryType':
            return _gql_type('Query')
        if field == 'mutationType':
            return _gql_type('Mutation')
        if field == 'subscriptionType':
            return None
        if field == 'types':
            names = (list(_GQL_TYPE_FIELDS) + list(_GQL_TYPE_ENUMS) +
                     sorted(_GQL_SCALARS))
            return [_gql_type(n) for n in names]
        if field == 'directives':
            return []
        return _MISSING

    if type_name == 'User':
        if field == 'login':
            return obj['login']
        if field == 'id':
            return node_id('User', obj['id'])
        if field == 'databaseId':
            return obj['id']
        if field == 'name':
            return obj['name']
        if field == 'email':
            return obj['email']
        if field == 'url':
            return f"/{obj['login']}"
        if field == 'avatarUrl':
            return ''
        if field in ('createdAt', 'updatedAt'):
            return '2020-01-01T00:00:00Z'
        if field == 'isViewer':
            return obj['login'] == VIEWER['login']
        return _MISSING

    if type_name == 'Repository':
        repo = obj
        if field == 'id':
            return node_id('Repository', repo.id)
        if field == 'databaseId':
            return repo.id
        if field == 'name':
            return repo.name
        if field == 'nameWithOwner':
            return repo.full_name
        if field == 'owner':
            return ('User', _gql_user_obj(repo.owner))
        if field == 'description':
            return repo.description
        if field in ('isPrivate', 'private'):
            return repo.private
        if field == 'isFork':
            return False
        if field == 'viewerPermission':
            return 'ADMIN'
        if field == 'hasIssuesEnabled':
            return True
        if field == 'hasPullRequestsEnabled':
            return True
        if field == 'defaultBranchRef':
            sha = repo.refs.get(f'refs/heads/{repo.default_branch}')
            if sha is None:
                return None
            return ('Ref', {'name': repo.default_branch, 'repo': repo,
                            'sha': sha})
        if field == 'issue':
            issue = repo.issues.get(int(args.get('number') or 0))
            if issue is None or issue.get('pull_request'):
                return None
            return ('Issue', (repo, issue))
        if field == 'pullRequest':
            issue = repo.issues.get(int(args.get('number') or 0))
            if issue is None or not issue.get('pull_request'):
                return None
            return ('PullRequest', (repo, issue))
        if field == 'issueOrPullRequest':
            issue = repo.issues.get(int(args.get('number') or 0))
            if issue is None:
                return None
            if issue.get('pull_request'):
                return ('PullRequest', (repo, issue))
            return ('Issue', (repo, issue))
        if field == 'issues':
            items = [i for i in repo.issues.values()
                     if not i.get('pull_request')]
            states = args.get('states')
            if states:
                items = [i for i in items if i['state'].upper() in states]
            items = _gql_order(items, args.get('orderBy'))
            tagged = [('Issue', (repo, i)) for i in items]
            return _gql_connection('IssueConnection', 'IssueEdge', tagged, args)
        if field == 'pullRequests':
            items = [i for i in repo.issues.values() if i.get('pull_request')]
            states = args.get('states') or ['OPEN']
            items = [i for i in items if _gql_pr_state(i) in states]
            items = _gql_order(items, args.get('orderBy'))
            tagged = [('PullRequest', (repo, i)) for i in items]
            return _gql_connection('PullRequestConnection', 'PullRequestEdge',
                                   tagged, args)
        if field == 'labels':
            tagged = [('Label', lbl) for lbl in repo.labels.values()]
            return _gql_connection('LabelConnection', 'LabelEdge', tagged, args)
        if field == 'createdAt':
            return iso(repo.created_at)
        if field == 'updatedAt':
            return iso(repo.updated_at)
        if field == 'pushedAt':
            return iso(repo.pushed_at)
        if field == 'url':
            return f'/{repo.full_name}'
        if field == 'homepageUrl':
            return None
        if field in ('stargazerCount', 'forkCount'):
            return 0
        if field == 'primaryLanguage':
            return None
        return _MISSING

    if type_name == 'Issue':
        repo, issue = obj
        if field == 'id':
            return node_id('Issue', issue['id'])
        if field == 'databaseId':
            return issue['id']
        if field == 'number':
            return issue['number']
        if field == 'title':
            return issue['title']
        if field in ('body', 'bodyText'):
            return issue.get('body') or ''
        if field == 'state':
            return 'OPEN' if issue['state'] == 'open' else 'CLOSED'
        if field == 'closed':
            return issue['state'] == 'closed'
        if field == 'locked':
            return False
        if field == 'author':
            return ('User', _gql_user_obj(issue['user']))
        if field == 'repository':
            return ('Repository', repo)
        if field == 'url':
            return f"/{repo.full_name}/issues/{issue['number']}"
        if field == 'createdAt':
            return iso(issue['created_at'])
        if field == 'updatedAt':
            return iso(issue['updated_at'])
        if field == 'closedAt':
            return iso(issue['closed_at']) if issue.get('closed_at') else None
        if field == 'labels':
            tagged = [('Label', repo.labels[name]) for name in issue['labels']
                      if name in repo.labels]
            return _gql_connection('LabelConnection', 'LabelEdge', tagged, args)
        if field == 'comments':
            tagged = [('IssueComment', (repo, issue, c))
                      for c in issue['comments']]
            return _gql_connection('IssueCommentConnection',
                                   'IssueCommentEdge', tagged, args)
        return _MISSING

    if type_name == 'PullRequest':
        repo, issue = obj
        pull = issue['pull']
        if field == 'id':
            return node_id('PullRequest', issue['id'])
        if field == 'databaseId':
            return issue['id']
        if field == 'number':
            return issue['number']
        if field == 'title':
            return issue['title']
        if field in ('body', 'bodyText'):
            return issue.get('body') or ''
        if field == 'state':
            return _gql_pr_state(issue)
        if field == 'merged':
            return bool(pull.get('merged'))
        if field == 'closed':
            return issue['state'] == 'closed'
        if field == 'isDraft':
            return False
        if field == 'author':
            return ('User', _gql_user_obj(issue['user']))
        if field == 'repository':
            return ('Repository', repo)
        if field == 'url':
            return f"/{repo.full_name}/pull/{issue['number']}"
        if field == 'headRefName':
            return pull['head_ref']
        if field == 'baseRefName':
            return pull['base_ref']
        if field == 'headRef':
            sha = _branch_tip(repo, pull['head_ref'])
            if sha is None:
                return None
            return ('Ref', {'name': pull['head_ref'], 'repo': repo,
                            'sha': sha})
        if field == 'baseRef':
            sha = _branch_tip(repo, pull['base_ref'])
            if sha is None:
                return None
            return ('Ref', {'name': pull['base_ref'], 'repo': repo,
                            'sha': sha})
        if field == 'mergeable':
            if pull.get('merged'):
                return 'MERGEABLE'
            result = _pr_mergeable(repo, pull)
            if result is None:
                return 'UNKNOWN'
            return 'MERGEABLE' if result else 'CONFLICTING'
        if field == 'files':
            tagged = _gql_pr_files(repo, issue)
            return _gql_connection('PullRequestChangedFileConnection',
                                   'PullRequestChangedFileEdge', tagged, args)
        if field in ('additions', 'deletions', 'changedFiles'):
            files = _gql_pr_files(repo, issue)
            if field == 'changedFiles':
                return len(files)
            if field == 'additions':
                return sum(f[1]['additions'] for f in files)
            return sum(f[1]['deletions'] for f in files)
        if field == 'mergeCommit':
            sha = pull.get('merge_commit_sha')
            return ('Commit', {'oid': sha}) if sha else None
        if field == 'mergedAt':
            return iso(pull['merged_at']) if pull.get('merged_at') else None
        if field == 'createdAt':
            return iso(issue['created_at'])
        if field == 'updatedAt':
            return iso(issue['updated_at'])
        if field == 'closedAt':
            return iso(issue['closed_at']) if issue.get('closed_at') else None
        if field == 'comments':
            tagged = [('IssueComment', (repo, issue, c))
                      for c in issue['comments']]
            return _gql_connection('IssueCommentConnection',
                                   'IssueCommentEdge', tagged, args)
        return _MISSING

    if type_name == 'Ref':
        if field == 'name':
            return obj['name']
        if field == 'prefix':
            return 'refs/heads/'
        if field == 'repository':
            return ('Repository', obj['repo'])
        if field == 'target':
            return ('Commit', {'oid': obj['sha']}) if obj.get('sha') else None
        return _MISSING

    if type_name == 'Commit':
        if field == 'oid':
            return obj['oid']
        if field == 'abbreviatedOid':
            return (obj['oid'] or '')[:7]
        return _MISSING

    if type_name == 'Label':
        if field == 'id':
            return node_id('Label', obj['id'])
        if field == 'name':
            return obj['name']
        if field == 'color':
            return obj['color']
        if field == 'description':
            return obj.get('description')
        return _MISSING

    if type_name == 'IssueComment':
        repo, issue, comment = obj
        if field == 'id':
            return node_id('IssueComment', comment['id'])
        if field == 'databaseId':
            return comment['id']
        if field in ('body', 'bodyText'):
            return comment['body']
        if field == 'author':
            return ('User', _gql_user_obj(comment['user']))
        if field == 'createdAt':
            return iso(comment['created_at'])
        if field == 'updatedAt':
            return iso(comment['updated_at'])
        if field == 'url':
            return (f"/{repo.full_name}/issues/{issue['number']}"
                    f"#issuecomment-{comment['id']}")
        return _MISSING

    # Dict payloads (connections, pageInfo, mutation payloads, edges) answer
    # their stored keys directly.
    if isinstance(obj, dict) and field in obj:
        return obj[field]
    return _MISSING


def _gql_mutation(field, args, gtx):
    inp = args.get('input') or {}
    if field == 'createIssue':
        repo = _repo_from_global_id(inp.get('repositoryId'))
        if repo is None:
            gtx['errors'].append({'message': 'Repository not found'})
            return ('CreateIssuePayload', {'issue': None})
        issue = _new_issue(repo, inp.get('title') or '', inp.get('body'), [])
        return ('CreateIssuePayload', {'issue': ('Issue', (repo, issue)),
                                       'clientMutationId': inp.get('clientMutationId')})
    if field in ('closeIssue', 'reopenIssue'):
        _kind, raw_id = _decode_global_id(inp.get('issueId'))
        repo, issue = _find_issue_by_raw_id(raw_id)
        if issue is None:
            gtx['errors'].append({'message': 'Issue not found'})
            return ('CloseIssuePayload', {'issue': None})
        now = tick()
        if field == 'closeIssue':
            issue['state'] = 'closed'
            issue['closed_at'] = now
        else:
            issue['state'] = 'open'
            issue['closed_at'] = None
        issue['updated_at'] = now
        payload_type = 'CloseIssuePayload' if field == 'closeIssue' else 'ReopenIssuePayload'
        return (payload_type, {'issue': ('Issue', (repo, issue)),
                               'clientMutationId': inp.get('clientMutationId')})
    if field == 'createPullRequest':
        repo = _repo_from_global_id(inp.get('repositoryId'))
        if repo is None:
            gtx['errors'].append({'message': 'Repository not found'})
            return ('CreatePullRequestPayload', {'pullRequest': None})
        head_ref = _pr_head_ref(inp.get('headRefName') or '')
        base_ref = _pr_head_ref(inp.get('baseRefName') or repo.default_branch)
        head_sha = _branch_tip(repo, head_ref)
        base_sha = _branch_tip(repo, base_ref)
        if head_sha is None or base_sha is None:
            gtx['errors'].append({'message': 'Invalid head or base branch'})
            return ('CreatePullRequestPayload', {'pullRequest': None})
        pull = {'head_ref': head_ref, 'base_ref': base_ref,
                'head_sha': head_sha, 'base_sha': base_sha,
                'merged': False, 'merged_at': None, 'merge_commit_sha': None,
                'mergeable': None}
        issue = _new_issue(repo, inp.get('title') or '', inp.get('body') or '',
                           [], is_pull=True, pull=pull)
        pull['mergeable'] = _pr_mergeable(repo, pull)
        return ('CreatePullRequestPayload',
                {'pullRequest': ('PullRequest', (repo, issue)),
                 'clientMutationId': inp.get('clientMutationId')})
    if field == 'mergePullRequest':
        _kind, raw_id = _decode_global_id(inp.get('pullRequestId'))
        repo, issue = _find_issue_by_raw_id(raw_id)
        if issue is None or not issue.get('pull_request'):
            gtx['errors'].append({'message': 'Pull request not found'})
            return ('MergePullRequestPayload', {'pullRequest': None})
        method = {'SQUASH': 'squash', 'MERGE': 'merge',
                  'REBASE': 'merge'}.get((inp.get('mergeMethod') or 'MERGE').upper(),
                                         'merge')
        _sha, err = _merge_pull(repo, issue, method)
        if err:
            gtx['errors'].append({'message': f'Pull request is not mergeable: {err}'})
        return ('MergePullRequestPayload',
                {'pullRequest': ('PullRequest', (repo, issue)),
                 'clientMutationId': inp.get('clientMutationId')})
    if field == 'addComment':
        _kind, raw_id = _decode_global_id(inp.get('subjectId'))
        repo, issue = _find_issue_by_raw_id(raw_id)
        if issue is None:
            gtx['errors'].append({'message': 'Subject not found'})
            return ('AddCommentPayload', {'commentEdge': None})
        now = tick()
        comment = {'id': next_id(), 'body': inp.get('body') or '',
                   'user': VIEWER['login'], 'created_at': now,
                   'updated_at': now}
        issue['comments'].append(comment)
        issue['updated_at'] = now
        edge = ('IssueCommentEdge',
                {'node': ('IssueComment', (repo, issue, comment)),
                 'cursor': _gql_cursor(len(issue['comments']) - 1)})
        return ('AddCommentPayload', {'commentEdge': edge,
                                      'clientMutationId': inp.get('clientMutationId')})
    return _MISSING


# --- executor ----------------------------------------------------------------

_NODE_TYPES = {'Repository', 'Issue', 'PullRequest', 'User', 'IssueComment',
               'Label'}


def _gql_type_matches(condition, runtime):
    if condition == runtime:
        return True
    if condition == 'Node':
        return runtime in _NODE_TYPES
    if condition in ('Actor', 'RepositoryOwner'):
        return runtime == 'User'
    return False


def _gql_complete(value, selection, gtx):
    if value is None:
        return None
    if isinstance(value, list):
        return [_gql_complete(v, selection, gtx) for v in value]
    if isinstance(value, tuple):
        if selection:
            return _gql_exec_set(value, selection, gtx)
        return None
    return value


def _gql_exec_set(tagged, selections, gtx):
    runtime, obj = tagged
    result = {}
    for sel in selections:
        if sel['kind'] == 'inline':
            if _gql_type_matches(sel['type'], runtime):
                result.update(_gql_exec_set((runtime, obj), sel['selection'], gtx))
            continue
        if sel['kind'] == 'spread':
            frag = gtx['fragments'].get(sel['name'])
            if frag and _gql_type_matches(frag['on'], runtime):
                result.update(_gql_exec_set((runtime, obj), frag['selection'], gtx))
            continue
        name = sel['name']
        key = sel['alias'] or name
        if name == '__typename':
            result[key] = runtime
            continue
        args = {k: _gql_value(v, gtx['variables'])
                for k, v in sel['args'].items()}
        try:
            value = _gql_resolve(runtime, obj, name, args, gtx)
        except Exception as exc:
            gtx['errors'].append({'message': str(exc)})
            result[key] = None
            continue
        if value is _MISSING:
            gtx['errors'].append(
                {'message': f"Field '{name}' doesn't exist on type '{runtime}'"})
            result[key] = None
            continue
        result[key] = _gql_complete(value, sel['selection'], gtx)
    return result


def _gql_execute(doc_text, variables, operation_name=None):
    try:
        ops = _GqlParser(doc_text or '').parse_document()
    except Exception as exc:
        return {'errors': [{'message': f'Syntax error: {exc}'}]}
    fragments = {op['name']: op for op in ops if op['type'] == 'fragment'}
    candidates = [op for op in ops if op['type'] in ('query', 'mutation',
                                                     'subscription')]
    op = None
    if operation_name:
        op = next((o for o in candidates if o['name'] == operation_name), None)
    if op is None and candidates:
        op = candidates[0]
    if op is None:
        return {'errors': [{'message': 'No operation found'}]}
    variables = dict(variables or {})
    for name, default in op['var_defs'].items():
        if name not in variables and default is not None:
            variables[name] = _gql_value(default, {})
    root_type = 'Mutation' if op['type'] == 'mutation' else 'Query'
    gtx = {'variables': variables, 'errors': [], 'fragments': fragments}
    data = _gql_exec_set((root_type, None), op['selection'], gtx)
    out = {'data': data}
    if gtx['errors']:
        out['errors'] = gtx['errors']
    return out


def handle_graphql(ctx):
    body = ctx.json
    with _store_lock:
        if isinstance(body, list):
            results = []
            for item in body:
                if isinstance(item, dict):
                    results.append(_gql_execute(item.get('query') or '',
                                                item.get('variables') or {},
                                                item.get('operationName')))
                else:
                    results.append({'errors': [{'message': 'Invalid operation'}]})
            return ctx.send(200, results)
        if isinstance(body, dict):
            result = _gql_execute(body.get('query') or '',
                                  body.get('variables') or {},
                                  body.get('operationName'))
            return ctx.send(200, result)
        return ctx.send(200, {'errors': [{'message': 'Invalid request body'}]})


MCP_TOOLS = [
    {
        'name': 'create_repo',
        'description': 'Create a new repository for the authenticated user.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string', 'description': 'The repository owner login.'},
                'name': {'type': 'string', 'description': 'The repository name.'},
                'description': {'type': 'string', 'description': 'A short description.'},
                'private': {'type': 'boolean', 'description': 'Whether the repository is private.'},
            },
            'required': ['name'],
        },
    },
    {
        'name': 'get_repo',
        'description': 'Get a repository by owner and name.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
            },
            'required': ['owner', 'repo'],
        },
    },
    {
        'name': 'create_issue',
        'description': 'Create an issue in a repository.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'title': {'type': 'string'},
                'body': {'type': 'string'},
                'labels': {'type': 'array', 'items': {'type': 'string'}},
            },
            'required': ['owner', 'repo', 'title'],
        },
    },
    {
        'name': 'get_issue',
        'description': 'Get one issue by number.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'issue_number': {'type': 'integer'},
            },
            'required': ['owner', 'repo', 'issue_number'],
        },
    },
    {
        'name': 'list_issues',
        'description': 'List issues in a repository.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'state': {'type': 'string', 'enum': ['open', 'closed', 'all']},
            },
            'required': ['owner', 'repo'],
        },
    },
    {
        'name': 'create_pull_request',
        'description': 'Create a pull request in a repository.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'title': {'type': 'string'},
                'body': {'type': 'string'},
                'head': {'type': 'string'},
                'base': {'type': 'string'},
            },
            'required': ['owner', 'repo', 'title', 'head', 'base'],
        },
    },
    {
        'name': 'list_pull_requests',
        'description': 'List pull requests in a repository.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'state': {'type': 'string', 'enum': ['open', 'closed', 'all']},
            },
            'required': ['owner', 'repo'],
        },
    },
    {
        'name': 'merge_pull_request',
        'description': 'Merge a pull request by number.',
        'inputSchema': {
            'type': 'object',
            'properties': {
                'owner': {'type': 'string'},
                'repo': {'type': 'string'},
                'pull_number': {'type': 'integer'},
            },
            'required': ['owner', 'repo', 'pull_number'],
        },
    },
]

MCP_REVISIONS = ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05']


def _tool_result(obj, is_error=False):
    text = obj if isinstance(obj, str) else json.dumps(obj)
    result = {'content': [{'type': 'text', 'text': text}], 'isError': is_error}
    if isinstance(obj, dict):
        result['structuredContent'] = obj
    return result


def _mcp_call_tool(name, args):
    """Run one tool; returns a JSON-RPC result object (tool errors use isError)."""
    args = args or {}
    owner = args.get('owner') or VIEWER['login']
    repo_name = args.get('repo')
    if name not in {t['name'] for t in MCP_TOOLS}:
        return None
    with _store_lock:
        if name == 'create_repo':
            rname = args.get('name')
            if not rname:
                return _tool_result('missing required argument: name', True)
            repo = STORE.get(VIEWER['login'], rname)
            if repo is None:
                repo = STORE.create(VIEWER['login'], rname, args.get('description') or '',
                                    args.get('private', False))
            return _tool_result(repo_json(repo))
        repo = STORE.get(owner, repo_name) if repo_name else None
        if name == 'get_repo':
            if repo is None:
                return _tool_result(f'repository {owner}/{repo_name} not found', True)
            return _tool_result(repo_json(repo))
        if repo is None:
            return _tool_result(f'repository {owner}/{repo_name} not found', True)
        if name == 'create_issue':
            title = args.get('title')
            if not title:
                return _tool_result('missing required argument: title', True)
            labels = args.get('labels') or []
            for lab in labels:
                if lab not in repo.labels:
                    return _tool_result(f'unknown label: {lab}', True)
            issue = _new_issue(repo, title, args.get('body'), labels)
            return _tool_result(issue_json(repo, issue))
        if name == 'get_issue':
            number = args.get('issue_number')
            issue = repo.issues.get(int(number)) if number is not None else None
            if issue is None:
                return _tool_result(f'issue {number} not found', True)
            return _tool_result(issue_json(repo, issue))
        if name == 'list_issues':
            state = args.get('state') or 'open'
            items = [i for i in repo.issues.values() if not i.get('pull_request')]
            if state in ('open', 'closed'):
                items = [i for i in items if i['state'] == state]
            items.sort(key=lambda i: i['created_at'], reverse=True)
            return _tool_result(json.dumps([issue_json(repo, i) for i in items]))
        if name == 'create_pull_request':
            title = args.get('title')
            head_ref = _pr_head_ref(args.get('head') or '')
            base_ref = _pr_head_ref(args.get('base') or repo.default_branch)
            head_sha = _branch_tip(repo, head_ref)
            base_sha = _branch_tip(repo, base_ref)
            if not title or head_sha is None or base_sha is None:
                return _tool_result('invalid pull request: title, head and base are required '
                                    'and both branches must exist', True)
            pull = {'head_ref': head_ref, 'base_ref': base_ref,
                    'head_sha': head_sha, 'base_sha': base_sha,
                    'merged': False, 'merged_at': None, 'merge_commit_sha': None,
                    'mergeable': None}
            issue = _new_issue(repo, title, args.get('body') or '', [],
                               is_pull=True, pull=pull)
            pull['mergeable'] = _pr_mergeable(repo, pull)
            return _tool_result(pull_json(repo, issue))
        if name == 'list_pull_requests':
            state = args.get('state') or 'open'
            items = [i for i in repo.issues.values() if i.get('pull_request')]
            if state in ('open', 'closed'):
                items = [i for i in items if i['state'] == state]
            items.sort(key=lambda i: i['created_at'], reverse=True)
            return _tool_result(json.dumps([pull_json(repo, i) for i in items]))
        if name == 'merge_pull_request':
            number = args.get('pull_number')
            issue = _get_pull(repo, number) if number is not None else None
            if issue is None:
                return _tool_result(f'pull request {number} not found', True)
            sha, err = _merge_pull(repo, issue, args.get('merge_method') or 'merge',
                                   args.get('commit_title'), args.get('commit_message'))
            if sha is None:
                return _tool_result(f'pull request is not mergeable ({err})', True)
            return _tool_result({'sha': sha, 'merged': True,
                                 'message': 'Pull Request successfully merged'})
    return None  # unknown tool


def _mcp_handle_one(msg):
    """One JSON-RPC message -> (status, response dict or None for notifications)."""
    if not isinstance(msg, dict):
        return 200, {'jsonrpc': '2.0', 'id': None,
                     'error': {'code': -32600, 'message': 'Invalid Request'}}
    method = msg.get('method')
    msg_id = msg.get('id')
    params = msg.get('params') or {}
    if msg_id is None:  # notification
        return 202, None
    if method == 'initialize':
        requested = params.get('protocolVersion')
        version = requested if requested in MCP_REVISIONS else MCP_REVISIONS[0]
        return 200, {'jsonrpc': '2.0', 'id': msg_id, 'result': {
            'protocolVersion': version,
            'capabilities': {'tools': {'listChanged': False}},
            'serverInfo': {'name': 'forge', 'version': '1.0.0'},
        }}
    if method == 'ping':
        return 200, {'jsonrpc': '2.0', 'id': msg_id, 'result': {}}
    if method == 'tools/list':
        return 200, {'jsonrpc': '2.0', 'id': msg_id, 'result': {'tools': MCP_TOOLS}}
    if method == 'tools/call':
        tool_name = params.get('name')
        result = _mcp_call_tool(tool_name, params.get('arguments'))
        if result is None:
            return 200, {'jsonrpc': '2.0', 'id': msg_id,
                         'error': {'code': -32602, 'message': f'Unknown tool: {tool_name}'}}
        return 200, {'jsonrpc': '2.0', 'id': msg_id, 'result': result}
    return 200, {'jsonrpc': '2.0', 'id': msg_id,
                 'error': {'code': -32601, 'message': f'Method not found: {method}'}}


def handle_mcp(ctx):
    if ctx.method == 'GET':
        return ctx.send_raw(405, b'', 'application/json')
    raw = ctx.body or b''
    try:
        payload = json.loads(raw.decode('utf-8'))
    except Exception:
        m = re.search(rb'"id"\s*:\s*(\d+|"[^"]*")', raw)
        rid = None
        if m:
            try:
                rid = json.loads(m.group(1))
            except Exception:
                rid = None
        return ctx.send(400, {'jsonrpc': '2.0', 'id': rid,
                              'error': {'code': -32700, 'message': 'Parse error'}})
    if isinstance(payload, list):
        responses = []
        for msg in payload:
            _status, resp = _mcp_handle_one(msg)
            if resp is not None:
                responses.append(resp)
        return ctx.send(200, responses)
    status, resp = _mcp_handle_one(payload)
    if resp is None:
        return ctx.send_raw(202, b'', 'application/json')
    return ctx.send(status, resp)


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------

REPO_RE = re.compile(r'^/api/v3/repos/([^/]+)/([^/]+)(/.*)?$')
GIT_RE = re.compile(r'^/([^/]+)/([^/]+)\.git(/.*)?$')


def dispatch_rest(ctx):
    path = ctx.path
    method = ctx.method
    if path in ('/api/v3', '/api/v3/'):
        if method == 'GET':
            return api_root(ctx)
        return ctx.error(404, 'Not Found')
    if path == '/api/v3/user':
        if method == 'GET':
            return get_user(ctx)
        return ctx.error(404, 'Not Found')
    if path == '/api/v3/meta':
        if method == 'GET':
            return api_meta(ctx)
        return ctx.error(404, 'Not Found')
    m = re.match(r'^/api/v3/users/([^/]+)$', path)
    if m:
        if method == 'GET':
            return get_user_by_login(ctx, m.group(1))
        return ctx.error(404, 'Not Found')
    if path == '/api/v3/user/repos':
        if method == 'POST':
            return create_repo(ctx)
        if method == 'GET':
            return list_user_repos(ctx)
        return ctx.error(404, 'Not Found')
    m = REPO_RE.match(path)
    if m:
        owner, name, tail = m.group(1), m.group(2), m.group(3) or ''
        if tail == '':
            if method == 'GET':
                return get_repo(ctx, owner, name)
            if method == 'PATCH':
                return patch_repo(ctx, owner, name)
            if method == 'DELETE':
                return delete_repo(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        if tail == '/branches':
            if method == 'GET':
                return list_branches(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        if tail.startswith('/branches/'):
            if method == 'GET':
                return get_branch(ctx, owner, name, tail[len('/branches/'):])
            return ctx.error(404, 'Not Found')
        if tail == '/commits':
            if method == 'GET':
                return list_commits(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        if tail.startswith('/commits/'):
            if method == 'GET':
                return get_commit(ctx, owner, name, tail[len('/commits/'):])
            return ctx.error(404, 'Not Found')
        if tail == '/contents' or tail.startswith('/contents/'):
            sub = tail[len('/contents'):] if tail.startswith('/contents/') else ''
            sub = sub.lstrip('/')
            if method == 'GET':
                return get_contents(ctx, owner, name, sub)
            if method == 'PUT':
                return put_contents(ctx, owner, name, sub)
            if method == 'DELETE':
                return delete_contents(ctx, owner, name, sub)
            return ctx.error(404, 'Not Found')
        if tail == '/issues':
            if method == 'POST':
                return create_issue(ctx, owner, name)
            if method == 'GET':
                return list_issues(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        im = re.match(r'^/issues/(\d+)$', tail)
        if im:
            if method == 'GET':
                return get_issue(ctx, owner, name, im.group(1))
            if method == 'PATCH':
                return patch_issue(ctx, owner, name, im.group(1))
            return ctx.error(404, 'Not Found')
        cm = re.match(r'^/issues/(\d+)/comments$', tail)
        if cm:
            if method == 'POST':
                return create_comment(ctx, owner, name, cm.group(1))
            if method == 'GET':
                return list_comments(ctx, owner, name, cm.group(1))
            return ctx.error(404, 'Not Found')
        if tail == '/labels':
            if method == 'GET':
                return list_labels(ctx, owner, name)
            if method == 'POST':
                return create_label(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        if tail == '/git/refs':
            if method == 'POST':
                return create_ref(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        if tail.startswith('/git/refs/'):
            sub = tail[len('/git/refs/'):]
            if method == 'GET':
                return get_ref(ctx, owner, name, sub)
            if method == 'DELETE':
                return delete_ref(ctx, owner, name, sub)
            return ctx.error(404, 'Not Found')
        if tail == '/pulls':
            if method == 'POST':
                return pulls_create(ctx, owner, name)
            if method == 'GET':
                return pulls_list(ctx, owner, name)
            return ctx.error(404, 'Not Found')
        pm = re.match(r'^/pulls/(\d+)$', tail)
        if pm:
            if method == 'GET':
                return pulls_get(ctx, owner, name, pm.group(1))
            if method == 'PATCH':
                return pulls_patch(ctx, owner, name, pm.group(1))
            return ctx.error(404, 'Not Found')
        fm = re.match(r'^/pulls/(\d+)/files$', tail)
        if fm:
            if method == 'GET':
                return pulls_files(ctx, owner, name, fm.group(1))
            return ctx.error(404, 'Not Found')
        mm = re.match(r'^/pulls/(\d+)/merge$', tail)
        if mm:
            if method == 'PUT':
                return pulls_merge(ctx, owner, name, mm.group(1))
            return ctx.error(404, 'Not Found')
        return ctx.error(404, 'Not Found')
    return ctx.error(404, 'Not Found')


def dispatch(ctx):
    path = ctx.path
    if path == '/healthz':
        if ctx.method == 'GET':
            return ctx.send(200, {'ok': True})
        return ctx.error(404, 'Not Found')
    if path.startswith('/api/v3'):
        if not authed(ctx):
            return ctx.error(401, 'Bad credentials')
        if ctx.json is None and ctx.body and ctx.method in ('POST', 'PATCH', 'PUT'):
            return ctx.error(400, 'Problems parsing JSON')
        return dispatch_rest(ctx)
    if path == '/api/graphql':
        if not authed(ctx):
            return ctx.error(401, 'Bad credentials')
        if ctx.method != 'POST':
            return ctx.error(404, 'Not Found')
        return handle_graphql(ctx)
    if path == '/mcp':
        if not authed(ctx):
            return ctx.error(401, 'Bad credentials')
        return handle_mcp(ctx)
    m = GIT_RE.match(path)
    if m:
        return handle_git(ctx, m.group(1), m.group(2), (m.group(3) or '').lstrip('/'))
    return ctx.error(404, 'Not Found')


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'forge'

    def setup(self):
        super().setup()
        # Responses go out as two small writes (headers, then body); without
        # TCP_NODELAY each exchange stalls on the peer's delayed ACK, which
        # costs tens of ms per request on a keep-alive connection.
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP,
                                       socket.TCP_NODELAY, 1)
        except OSError:
            pass

    def log_message(self, fmt, *args):
        pass

    def _handle(self, method):
        try:
            parsed = urllib.parse.urlsplit(self.path)
            path = urllib.parse.unquote(parsed.path)
            query = urllib.parse.parse_qs(parsed.query)
            length = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(length) if length > 0 else b''
            if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
                body = self._read_chunked()
            ctx = Ctx(self, method, path, query, body, self.headers)
            dispatch(ctx)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # never take the server down for one request
            try:
                ctx = Ctx(self, method, '/', {}, b'', self.headers)
                ctx.error(500, f'Internal error: {exc}')
            except Exception:
                pass

    def _read_chunked(self):
        out = bytearray()
        while True:
            line = self.rfile.readline(65536).strip()
            try:
                n = int(line.split(b';')[0], 16)
            except ValueError:
                break
            if n == 0:
                while True:
                    trailer = self.rfile.readline(65536)
                    if trailer in (b'\r\n', b'\n', b''):
                        break
                break
            out += self.rfile.read(n)
            self.rfile.read(2)
        return bytes(out)

    def do_GET(self):
        self._handle('GET')

    def do_POST(self):
        self._handle('POST')

    def do_PUT(self):
        self._handle('PUT')

    def do_PATCH(self):
        self._handle('PATCH')

    def do_DELETE(self):
        self._handle('DELETE')

    def do_HEAD(self):
        self._handle('HEAD')

    def do_OPTIONS(self):
        self._handle('OPTIONS')


# --------------------------------------------------------------------------
# Lifecycle: register with the env, poll /done, exit 0. (--serve for local.)
# --------------------------------------------------------------------------


def _env_post(url, payload, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={'content-type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return res.status


def _env_get_json(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as res:
        return json.loads(res.read().decode())


def _retry_delay(attempt):
    return min(2.0, 0.25 * (2 ** min(attempt, 8)))


class ForgeServer(ThreadingHTTPServer):
    daemon_threads = True
    # socketserver's default listen backlog is 5: a burst of concurrent
    # clones overflows it, SYNs drop, and clients stall on retransmit
    # timers. A forge must absorb connection bursts like Node's server.
    request_queue_size = 256


def main():
    serve_port = None
    if '--serve' in sys.argv:
        serve_port = int(sys.argv[sys.argv.index('--serve') + 1])
    env_url = (os.environ.get('MAKEFUN_ENV_URL') or '').rstrip('/')
    server = ForgeServer(('0.0.0.0', serve_port or 0), Handler)
    server.daemon_threads = True
    port = server.server_address[1]
    sys.stderr.write(f'[entry] listening on port {port}\n')
    if serve_port is not None or not env_url:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    # register (first one wins; 200 or 409 both mean "an entry is registered")
    attempt = 0
    while True:
        try:
            status = _env_post(f'{env_url}/register', {'port': port})
            if status in (200, 409):
                break
            if status in (400, 405):
                sys.stderr.write(f'[entry] register refused: HTTP {status}\n')
                return 1
        except Exception:
            pass
        time.sleep(_retry_delay(attempt))
        attempt += 1
    sys.stderr.write('[entry] registered; waiting for done\n')
    attempt = 0
    while True:
        try:
            body = _env_get_json(f'{env_url}/done')
            if body.get('done') is True:
                break
        except Exception:
            pass
        time.sleep(_retry_delay(attempt))
        attempt += 1
    threading.Thread(target=server.shutdown, daemon=True).start()
    sys.stderr.write('[entry] done\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
