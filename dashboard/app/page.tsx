import Refresh from "./refresh";

export const dynamic = "force-dynamic";

// Mirrors worker.NodeStatus / HandoffRecord in internal/worker.
type Handoff = { node: string; epoch: number; targets: number; sentAt: string; acked: boolean; error?: string };
type NodeStatus = {
  node: string; isLeader: boolean; leaderID: string; term: number; epoch: number; handoffEpoch: number;
  voters: string[]; healthy: boolean; owned: string[]; owners: Record<string, string>; handoffs?: Handoff[];
};

async function fetchNode(url: string): Promise<{ url: string; s?: NodeStatus; err?: string }> {
  try {
    const r = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(3000) });
    if (!r.ok) return { url, err: `HTTP ${r.status}` };
    return { url, s: await r.json() };
  } catch (e) {
    return { url, err: String(e) };
  }
}

const QUERIES: Record<string, string> = {
  "Unowned targets": "max(distriscrape_unowned_targets)",
  "Ring rebalances": "sum(distriscrape_ring_rebalances_total)",
  "Webhook failures": "sum(distriscrape_webhook_failures_total)",
  "Release p95 (s)": "histogram_quantile(0.95, sum by (le) (rate(distriscrape_release_to_webhook_seconds_bucket[5m])))",
};

async function prom(base: string, q: string): Promise<string> {
  try {
    const r = await fetch(`${base}/api/v1/query?query=${encodeURIComponent(q)}`, { cache: "no-store", signal: AbortSignal.timeout(3000) });
    const v = (await r.json()).data?.result?.[0]?.value?.[1];
    return v === undefined || v === "NaN" ? "-" : String(Math.round(Number(v) * 1000) / 1000);
  } catch {
    return "n/a";
  }
}

export default async function Page() {
  const promURL = process.env.PROMETHEUS_URL;
  const stats = promURL
    ? await Promise.all(Object.entries(QUERIES).map(async ([k, q]) => [k, await prom(promURL, q)] as const))
    : [];
  const urls = (process.env.WORKER_STATUS_URLS ?? "").split(",").map((u) => u.trim()).filter(Boolean);
  const nodes = await Promise.all(urls.map(fetchNode));
  const leader = nodes.find((n) => n.s?.isLeader)?.s;
  const owners = Object.entries(leader?.owners ?? {}).sort();

  return (
    <main>
      <Refresh ms={5000} />
      <h1>Distriscrape cluster</h1>
      {urls.length === 0 && <p>Set WORKER_STATUS_URLS (see .env.example).</p>}
      <p>
        Leader: <b>{leader?.node ?? "none reachable"}</b> · term {leader?.term ?? "-"} · epoch {leader?.epoch ?? "-"}
      </p>

      {stats.length > 0 && (
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          {stats.map(([k, v]) => (
            <div key={k} style={{ border: "1px solid #8884", borderRadius: 8, padding: "8px 14px" }}>
              <div style={{ fontSize: 12, opacity: 0.7 }}>{k}</div>
              <div style={{ fontSize: 24 }}>{v}</div>
            </div>
          ))}
        </div>
      )}

      <h2>Nodes</h2>
      <table cellPadding={6} border={1} style={{ borderCollapse: "collapse" }}>
        <thead><tr><th>Node</th><th>Role</th><th>Healthy</th><th>Epoch</th><th>Owned</th></tr></thead>
        <tbody>
          {nodes.map(({ url, s, err }) =>
            s ? (
              <tr key={url}>
                <td>{s.node}</td><td>{s.isLeader ? "leader" : "follower"}</td>
                <td>{s.healthy ? "✅" : "❌"}</td><td>{s.epoch}</td><td>{s.owned.length}</td>
              </tr>
            ) : (
              <tr key={url}><td colSpan={5}>❌ {url}: {err}</td></tr>
            ),
          )}
        </tbody>
      </table>

      <h2>Target ownership</h2>
      <table cellPadding={6} border={1} style={{ borderCollapse: "collapse" }}>
        <thead><tr><th>Target</th><th>Owner</th></tr></thead>
        <tbody>{owners.map(([t, o]) => <tr key={t}><td>{t}</td><td>{o}</td></tr>)}</tbody>
      </table>

      <h2>Leader handoffs</h2>
      <table cellPadding={6} border={1} style={{ borderCollapse: "collapse" }}>
        <thead><tr><th>Node</th><th>Epoch</th><th>Targets</th><th>Sent</th><th>Acked</th><th>Error</th></tr></thead>
        <tbody>
          {(leader?.handoffs ?? []).map((h) => (
            <tr key={h.node}>
              <td>{h.node}</td><td>{h.epoch}</td><td>{h.targets}</td>
              <td>{new Date(h.sentAt).toLocaleTimeString()}</td><td>{h.acked ? "✅" : "❌"}</td><td>{h.error}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
