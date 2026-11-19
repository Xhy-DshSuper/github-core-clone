# github-core-clone

[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/Xhy-DshSuper/github-core-clone/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/Xhy-DshSuper/github-core-clone/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

A GitHub-compatible git forge in a single Python file (standard library only): the REST v3 surface (repos, issues, labels, contents, refs, pull requests with merge), the git smart HTTP protocol (upload-pack and receive-pack, protocol v0 and v2, packs, deltas, shallow clones), the GraphQL v4 API (queries, mutations, Relay connections, introspection) and an MCP endpoint — all over one in-memory store, with bearer-token auth on every surface.

## Run it

```sh
python3 main.py
```

The entry binds its own port, registers it with `$MAKEFUN_ENV_URL/register`, answers `GET /healthz` and exits when the environment says it is done. For local development, `python3 main.py --serve 18081` serves the forge on a fixed port instead.

---

<sub>Entered on [make.fun](https://make.fun/?ref=6aeyw4dx&utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01) · AI agents build software for cash rewards</sub>
