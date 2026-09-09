/**
 * Error vocabulary of the Agent Graph supervisor tools (Maka port, slice P4).
 *
 * Every model-facing failure carries a stable machine `code` (DSH convention,
 * cf. {@link GraphControlError}); the tool layer prefixes messages with the
 * code so the model sees `[CODE] message` (tool-git's `[CODE]` style).
 * @module
 */

/** Stable machine codes of the supervisor tool surface. */
export type AgentGraphToolErrorCode =
  | 'invalid_input'
  | 'unknown_graph'
  | 'nothing_to_yield'
  | 'graph_closed'
  | 'not_root_session'
  | 'configuration'

/** Base error of the supervisor tool surface. */
export class AgentGraphToolError extends Error {
  readonly code: AgentGraphToolErrorCode

  constructor(code: AgentGraphToolErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.name = 'AgentGraphToolError'
    this.code = code
  }
}

/** Caller identity is not the graph's root session (root-only tools). */
export class AgentGraphNotRootSessionError extends AgentGraphToolError {
  constructor(graphId: string, rootSessionId: string, callingSessionId: string) {
    super(
      'not_root_session',
      `agent graph ${graphId}: only the root session ${rootSessionId} may call this tool (caller ${callingSessionId})`,
    )
    this.name = 'AgentGraphNotRootSessionError'
  }
}

/** The tool input is malformed or violates a Maka schedule bound. */
export class AgentGraphInvalidInputError extends AgentGraphToolError {
  constructor(message: string, options?: { cause?: unknown }) {
    super('invalid_input', message, options)
    this.name = 'AgentGraphInvalidInputError'
  }
}

/** The named graph has no durable rows in the store (no registry exists yet). */
export class AgentGraphUnknownGraphError extends AgentGraphToolError {
  constructor(graphId: string) {
    super('unknown_graph', `agent graph ${graphId} is not known to the controller`)
    this.name = 'AgentGraphUnknownGraphError'
  }
}

/** `yield_agent_graph` was called with nothing pending to yield for. */
export class AgentGraphNothingToYieldError extends AgentGraphToolError {
  constructor(graphId: string, detail: string) {
    super('nothing_to_yield', `agent graph ${graphId}: nothing to yield for — ${detail}`)
    this.name = 'AgentGraphNothingToYieldError'
  }
}

/** The graph is already finished; the operation is no longer valid. */
export class AgentGraphClosedError extends AgentGraphToolError {
  constructor(graphId: string, operation: string) {
    super('graph_closed', `agent graph ${graphId} is already finished; ${operation} is no longer valid`)
    this.name = 'AgentGraphClosedError'
  }
}
