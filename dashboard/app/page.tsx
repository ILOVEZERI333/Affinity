import Refresh from "./refresh";
import { QUERIES, fetchNode, parseUrls, pickLeader, prom } from "../lib/cluster";

export const dynamic = "force-dynamic";

export default async function Page() {
  const promURL = process.env.PROMETHEUS_URL;
  const urls = parseUrls(process.env.WORKER_STATUS_URLS);
  // Start both groups before awaiting either so a slow Prometheus can't delay worker status.
  const statsP = promURL
    ? Promise.all(Object.entries(QUERIES).map(async ([k, q]) => [k, await prom(promURL, q)] as const))
    : Promise.resolve([]);
  const nodesP = Promise.all(urls.map((u) => fetchNode(u)));
  const [stats, nodes] = await Promise.all([statsP, nodesP]);
  const leader = pickLeader(nodes);
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
