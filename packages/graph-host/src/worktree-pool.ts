/**
 * Worktree pool of the Agent Graph host assembly: adopts an existing pool slot
 * on miss and mints a new one keyed by the deterministic provision key.
 * @module
 */

import type {
  GraphOperatorWorktreePool,
  GraphOperatorWorktreeLease,
} from '@hy-sde-org/dsh-graph-executor'
import type { GraphHostWorktrees } from './types.ts'

/**
 * Process-local worktree pool. The lease id IS the provision key (the durable
 * `graph_operator_lease_<hash>` row value), so a host restart re-adopts the
 * same slot: the worktree was cut at a named branch equal to the lease key and
 * the pool state records that holder, which survives a lost state file as the
 * branch of a recovered entry.
 *
 * Release is a no-op on purpose: worktrees of terminal operators survive (a
 * later slice owns graph teardown; the binding row stays authoritative).
 */
export class GraphHostWorktreePool implements GraphOperatorWorktreePool {
  private readonly leases = new Map<string, GraphOperatorWorktreeLease>()

  constructor(private readonly worktrees: GraphHostWorktrees) {}

  async acquire(leaseKey: string): Promise<GraphOperatorWorktreeLease> {
    const existing = this.leases.get(leaseKey)
    if (existing !== undefined) return existing

    const entries = await this.worktrees.list()
    const adopted = entries.find(
      entry =>
        entry.exists &&
        (entry.branch === leaseKey || entry.leaseHolder === leaseKey),
    )
    if (adopted !== undefined) {
      const lease: GraphOperatorWorktreeLease = {
        leaseId: leaseKey,
        path: adopted.path,
        repoRoot: this.worktrees.repoRoot,
      }
      this.leases.set(leaseKey, lease)
      return lease
    }

    const acquired = await this.worktrees.acquire({
      holder: leaseKey,
      branch: leaseKey,
    })
    const lease: GraphOperatorWorktreeLease = {
      leaseId: leaseKey,
      path: acquired.path,
      repoRoot: acquired.repoRoot,
    }
    this.leases.set(leaseKey, lease)
    return lease
  }

  release(lease: GraphOperatorWorktreeLease): Promise<void> {
    // No-op: see the class comment. Keep the bound value referenced so the
    // signature stays honest about the dropped ownership.
    void lease
    return Promise.resolve()
  }
}
