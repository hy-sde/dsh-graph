/**
 * Committed-event → graph records fold (Maka `stream-graph-projection`, port
 * subset). DSH has no immutable cross-session event ledger, so the record
 * source is an adapter contract (P3 wires it to child sessions); this module
 * defines the deterministic record shape and replay rules: partial events are
 * ignored, one terminal facet per activation, no records after a terminal,
 * deterministic ids (Maka formula) and total order.
 * @module
 */

import { stableHash32 } from './hash.ts'
import { compareAgentGraphIdentity } from './identity.ts'
import type { AgentGraphRecord, AgentGraphRecordFacet } from './types.ts'
export function graphRecordId(
  graphId: string,
  operatorId: string,
  sessionId: string,
  runId: string,
  runtimeEventId: string,
): string {
  return `graph_record_${stableHash32({ graphId, operatorId, sessionId, runId, runtimeEventId })}`
}

/** One durable source event as the adapter exposes it (DSH: child-session output bound to a runtime event). */
export interface AgentGraphRecordSourceEvent {
  readonly runtimeEventId: string
  readonly seq: number
  readonly runId: string
  readonly summary: string
  readonly terminal: boolean
  readonly partial?: boolean
  readonly facets?: readonly AgentGraphRecordFacet[]
  readonly emittedAt: number
}

/** Adapter contract: whatever the host can enumerate as committed output for one operator session. */
export interface AgentGraphRecordSource {
  listCommittedEvents(
    operatorId: string,
    sessionId: string,
  ): Promise<readonly AgentGraphRecordSourceEvent[]>
}

export interface AgentGraphProjectionState {
  readonly records: readonly AgentGraphRecord[]
  readonly omittedPartialCount: number
  readonly operators: readonly {
    operatorId: string
    sessionId: string
    terminal: boolean
  }[]
}

/**
 * Fold committed source events into graph records. Validation per activation
 * (sessionId+runId): skip partial events (counted), at most one terminal, no
 * records after the terminal event. Record ids use Maka's formula so replays
 * across processes produce identical rows.
 */
export async function readCommittedAgentGraphProjection(
  graphId: string,
  operators: readonly { operatorId: string; sessionId: string }[],
  source: AgentGraphRecordSource,
): Promise<AgentGraphProjectionState> {
  const records: AgentGraphRecord[] = []
  let omittedPartialCount = 0
  const operatorStates: {
    operatorId: string
    sessionId: string
    terminal: boolean
  }[] = []

  for (const operator of operators) {
    const events = await source.listCommittedEvents(
      operator.operatorId,
      operator.sessionId,
    )
    const byRun = new Map<string, AgentGraphRecordSourceEvent[]>()
    for (const event of events) {
      const list = byRun.get(event.runId) ?? []
      list.push(event)
      byRun.set(event.runId, list)
    }
    let terminal = false
    for (const [runId, runEvents] of byRun) {
      const sorted = [...runEvents].sort((a, b) => a.seq - b.seq)
      let terminalSeen = false
      for (const event of sorted) {
        if (event.partial === true) {
          omittedPartialCount += 1
          continue
        }
        if (terminalSeen) continue
        records.push({
          recordId: graphRecordId(
            graphId,
            operator.operatorId,
            operator.sessionId,
            runId,
            event.runtimeEventId,
          ),
          graphId,
          operatorId: operator.operatorId,
          source: { sessionId: operator.sessionId, runId, seq: event.seq },
          summary: event.summary,
          facets: event.facets ?? messageFacetFor(event),
          emittedAt: event.emittedAt,
        })
        if (event.terminal) {
          terminalSeen = true
          terminal = true
        }
      }
    }
    operatorStates.push({
      operatorId: operator.operatorId,
      sessionId: operator.sessionId,
      terminal,
    })
  }

  records.sort(
    (a, b) =>
      a.emittedAt - b.emittedAt ||
      compareAgentGraphIdentity(a.operatorId, b.operatorId) ||
      compareAgentGraphIdentity(a.source.runId, b.source.runId) ||
      a.source.seq - b.source.seq ||
      compareAgentGraphIdentity(a.recordId, b.recordId),
  )
  return { records, omittedPartialCount, operators: operatorStates }
}

function messageFacetFor(
  event: AgentGraphRecordSourceEvent,
): readonly AgentGraphRecordFacet[] {
  return event.terminal ? ['message', 'terminal'] : ['message']
}
