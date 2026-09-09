# dsh-graph

Standalone port of the Maka Agent Graph family (control store, stream core, executor, supervisor tools, wakes, projection, host) for the DeepSeek Harness — durable dependency scheduling with exactly-once claims and supervisor yield/wake.

## Packages

- `@hy-sde-org/dsh-graph-control`
- `@hy-sde-org/dsh-graph-stream`
- `@hy-sde-org/dsh-graph-executor`
- `@hy-sde-org/dsh-tool-graph`
- `@hy-sde-org/dsh-graph-wakes`
- `@hy-sde-org/dsh-graph-projection`
- `@hy-sde-org/dsh-graph-host`

Mount by adding the package rows to a profile composition (see the harness
plugin docs); nothing here requires unpublished fork packages.
