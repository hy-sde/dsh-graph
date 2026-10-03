<!-- MIRROR-NOTE:START -->
> [!NOTE]
> 📦 This plugin lives in the [**dsh-plugins**](https://github.com/hy-sde/dsh-plugins) monorepo — file issues & pull requests there.
> npm: [`@hy-sde-org/dsh-graph-control`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-control) · [`@hy-sde-org/dsh-graph-executor`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-executor) · [`@hy-sde-org/dsh-graph-host`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-host) · [`@hy-sde-org/dsh-graph-projection`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-projection) · [`@hy-sde-org/dsh-graph-stream`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-stream) · [`@hy-sde-org/dsh-graph-wakes`](https://www.npmjs.com/package/@hy-sde-org/dsh-graph-wakes) · [`@hy-sde-org/dsh-tool-graph`](https://www.npmjs.com/package/@hy-sde-org/dsh-tool-graph)
<!-- MIRROR-NOTE:END -->

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
