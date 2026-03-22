# Contributing

## Commit Convention

Follow [Conventional Commits](https://www.conventionalcommits.org):

```
<type>(<scope>): <description>
```

### Types

| Type | Description |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `perf` | Performance improvement |
| `refactor` | Code restructuring without behavior change |
| `docs` | Documentation only |
| `test` | Adding or updating tests |
| `chore` | Build, CI, dependencies |
| `style` | Formatting, clippy fixes |

### Scopes (optional)

`wal`, `tokio`, `segment`, `crc`, `meta`, `bench`

### Examples

```
feat(segment): add parallel recovery
fix(tokio): add rt feature for spawn
perf(wal): use incremental CRC32C to avoid allocation
docs: update README with benchmark results
style: fix clippy warnings
chore: bump criterion to 0.8
```

## Before Submitting

```sh
cargo test --all-features
cargo clippy --all-features -- -D warnings
cargo bench
```
