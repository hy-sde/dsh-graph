/**
 * Process-local operator record fold for the Agent Graph host assembly.
 *
 * Decision (documented in the README): the P3 executor's `recordSink` receives
 * `AgentGraphRecordSourceEvent` WITHOUT operator/session identity (the event
 * carries `runId` only), so the host cannot attribute a terminal event to a
 * child-session log without changing P3. A durable `graph/record` event on a
 * child session would additionally be refused by the persistence read path
 * until `KNOWN_SESSION_EVENT_TYPES` is regenerated (P6 scope). Records are
 * derived state — the control rows (schedule, claims, provisions, wakes) are
 * the durable authority — so this slice keeps the P2 in-memory record fold and
 * documents that a host restart loses operator records until a later slice
 * adds the attributed durable event.
 * @module
 */

import type {
  AgentGraphRecordSource,
  AgentGraphRecordSourceEvent,
} from '@hy-sde-org/dsh-graph-stream'

/** Identity of one operator activation, recorded at child-run start. */
export interface GraphRecordIdentity {
  readonly graphId: string
  readonly operatorId: string
  readonly sessionId: string
}

/**
 * Maps the executor's identity-less terminal event to its activation identity
 * via the run id (the executor copies `claim.targetRunId` into both
 * `GraphOperatorChildStartInput.runId` and the emitted event).
 */
export class GraphRunIdentityLedger {
  private readonly identities = new Map<string, GraphRecordIdentity>()

  record(runId: string, identity: GraphRecordIdentity): void {
    this.identities.set(runId, identity)
  }

  /** Remove and return one identity; returns undefined after it was consumed. */
  take(runId: string): GraphRecordIdentity | undefined {
    const identity = this.identities.get(runId)
    this.identities.delete(runId)
    return identity
  }
}

/**
 * In-process record fold keyed by operator×session, satisfying
 * {@link AgentGraphRecordSource}. The host submits the same committed source
 * events the projection fold eats, so record derivation stays P2-identical
 * within one host process.
 */
export class InProcessGraphRecordSource implements AgentGraphRecordSource {
  private readonly byKey = new Map<string, AgentGraphRecordSourceEvent[]>()

  /** Append one terminal source event under an activation identity. */
  submit(identity: GraphRecordIdentity, event: AgentGraphRecordSourceEvent): void {
    const key = `${identity.operatorId}\u0000${identity.sessionId}`
    const events = this.byKey.get(key) ?? []
    events.push(event)
    this.byKey.set(key, events)
  }

  listCommittedEvents(
    operatorId: string,
    sessionId: string,
  ): Promise<readonly AgentGraphRecordSourceEvent[]> {
    return Promise.resolve([...(this.byKey.get(`${operatorId}\u0000${sessionId}`) ?? [])])
  }
}
