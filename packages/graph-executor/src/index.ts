/**
 * Child-operator executor adapter for the Agent Graph (Maka port, slice P3):
 * the coordinator-facing execution surface over injected subagent-runner and
 * worktree-pool seams, with durable operator worktree bindings in the control
 * store.
 * @module @hy-sde-org/dsh-graph-executor
 */

export {
  AgentGraphOperatorExecutor,
  createGraphOperatorExecutor,
  provisionKey,
} from './executor.ts'
export type {
  AgentGraphChildRun,
  AgentGraphOperatorExecutorOptions,
  GraphOperatorChildRunner,
  GraphOperatorChildStartInput,
  GraphOperatorWorktreeLease,
  GraphOperatorWorktreePool,
} from './types.ts'
