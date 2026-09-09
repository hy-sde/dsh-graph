/**
 * Function plugin registering the `graph` projection unit: the session's
 * standing agent-graph snapshot served through the session-projection seam —
 * registry snapshot, change feed, and every projection carrier — so a client
 * can render the graph rail from whole published values without coupling to
 * the graph control store. The plugin owns only the fold; delivery is the
 * seam's, and the host (P7) publishes `graph/change` events.
 *
 * @module @hy-sde-org/dsh-graph-projection
 */

import type { Context } from '@deepseek-ai/cordis'
import { graphProjectionDefinition } from './projection.ts'

export type * from './types.ts'

/** Cordis plugin name. */
export const name = 'graph-projection'
/** The projection registry is the plugin's whole purpose; without it the fiber stays pending. */
export const inject = ['sessionProjections']

/**
 * Register the `graph` unit; the registration is an effect on this plugin's
 * fiber, so unloading removes the key.
 * @param ctx - registrant context carrying the projection registry.
 */
export function apply(ctx: Context): void {
  ctx.sessionProjections.register(graphProjectionDefinition)
}
