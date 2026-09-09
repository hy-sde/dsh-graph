/**
 * Agent Graph host assembly (Maka port, slices P6–P7a): opens the graph
 * control store, wires the executor's child runner to `subagents` and the
 * worktree engine, feeds the P3 record sink through the run-identity ledger,
 * publishes the `agentGraphController` service the supervisor tools consume,
 * and drives wake delivery on the root session's idle boundaries.
 * @module
 */

export {
  createGraphHostServices,
  GraphHostContextOverflowError,
  isContextOverflow,
} from './assembler.ts'
export { GraphHostChildRunner } from './child-runner.ts'
export {
  buildSessionGraphProjection,
  SESSION_PROJECTION_INSTRUCTION_MAX_CHARS,
  SESSION_PROJECTION_MAX_RECORDS,
  SESSION_PROJECTION_MAX_WORK,
  SESSION_PROJECTION_SCHEMA_VERSION,
} from './projection.ts'
export {
  GraphRunIdentityLedger,
  InProcessGraphRecordSource,
} from './records.ts'
export { GraphHostWorktreePool } from './worktree-pool.ts'
export {
  Config,
  apply,
  inject,
  name,
  SERVICE_AGENT_GRAPH_CONTROLLER,
  SERVICE_GRAPH_HOST,
} from './plugin.ts'
export type * from './types.ts'
