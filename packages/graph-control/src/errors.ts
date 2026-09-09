/**
 * Error vocabulary of the Agent Graph control plane.
 *
 * `GraphControlError` carries a stable machine `code` (DSH convention, cf.
 * `StorageError`); the two schedule errors keep Maka's class names so the
 * reconciler (slice P2) can catch them by identity.
 * @module
 */

export type GraphControlErrorCode =
  | 'schedule-revision-conflict'
  | 'schedule-closed'
  | 'schedule-update-conflict'
  | 'intent-claim-conflict'
  | 'intent-not-found'
  | 'provision-conflict'
  | 'provision-not-found'
  | 'binding-conflict'
  | 'wake-not-found'
  | 'wake-attempt-not-found'
  | 'wake-already-delivered'
  | 'malformed-state'

export class GraphControlError extends Error {
  readonly code: GraphControlErrorCode

  constructor(code: GraphControlErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.name = 'GraphControlError'
    this.code = code
  }
}

/** A reconciler tried to admit work from an observation no longer current (Maka: `AgentGraphScheduleRevisionConflictError`). */
export class AgentGraphScheduleRevisionConflictError extends GraphControlError {
  readonly graphId: string
  readonly expectedRevision: number
  readonly currentRevision: number

  constructor(graphId: string, expectedRevision: number, currentRevision: number) {
    super(
      'schedule-revision-conflict',
      `Agent graph schedule ${graphId} revision changed from ${expectedRevision} to ${currentRevision}`,
    )
    this.name = 'AgentGraphScheduleRevisionConflictError'
    this.graphId = graphId
    this.expectedRevision = expectedRevision
    this.currentRevision = currentRevision
  }
}

/** Fresh admission after terminal closure (Maka: `AgentGraphScheduleClosedError`). Existing claims stay recoverable. */
export class AgentGraphScheduleClosedError extends GraphControlError {
  readonly graphId: string

  constructor(graphId: string) {
    super('schedule-closed', `Agent graph schedule ${graphId} is already finished`)
    this.name = 'AgentGraphScheduleClosedError'
    this.graphId = graphId
  }
}

/** Two different decisions claimed the same intent / target identity. */
export class AgentGraphIntentClaimConflictError extends GraphControlError {
  constructor(message: string) {
    super('intent-claim-conflict', message)
    this.name = 'AgentGraphIntentClaimConflictError'
  }
}
