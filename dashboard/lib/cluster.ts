import queries from "./queries.json";

// Mirrors worker.NodeStatus / HandoffRecord in internal/worker. testdata/status.json
// is checked against the Go structs by TestStatusFixtureMatchesGoStruct.
export type Handoff = { node: string; epoch: number; targets: number; sentAt: string; acked: boolean; error?: string };
export type NodeStatus = {
  node: string; isLeader: boolean; leaderID: string; term: number; epoch: number; handoffEpoch: number;
  voters: string[]; healthy: boolean; owned: string[]; owners: Record<string, string>; handoffs?: Handoff[];
};
export type NodeResult = { url: string; s?: NodeStatus; err?: string };

type Fetch = typeof fetch;

export const QUERIES: Record<string, string> = queries;

export function parseUrls(raw: string | undefined): string[] {
  return (raw ?? "").split(",").map((u) => u.trim()).filter(Boolean);
}

// Only what rendering and leader selection dereference; extra fields pass through.
function isNodeStatus(v: unknown): v is NodeStatus {
  const s = v as Partial<NodeStatus> | null;
  return (
    typeof s === "object" && s !== null &&
    typeof s.node === "string" && typeof s.isLeader === "boolean" && typeof s.healthy === "boolean" &&
    typeof s.term === "number" && typeof s.epoch === "number" &&
    Array.isArray(s.owned) &&
    (s.owners === undefined || (typeof s.owners === "object" && s.owners !== null)) &&
    (s.handoffs === undefined || Array.isArray(s.handoffs))
  );
}

export async function fetchNode(url: string, f: Fetch = fetch): Promise<NodeResult> {
  try {
    const r = await f(url, { cache: "no-store", signal: AbortSignal.timeout(3000) });
    if (!r.ok) return { url, err: `HTTP ${r.status}` };
    const body: unknown = await r.json();
    return isNodeStatus(body) ? { url, s: body } : { url, err: "malformed status response" };
  } catch (e) {
    return { url, err: String(e) };
  }
}

// During an election or partition more than one node can claim leadership; the
// claim with the highest term is the live one.
export function pickLeader(nodes: NodeResult[]): NodeStatus | undefined {
  let best: NodeStatus | undefined;
  for (const { s } of nodes) {
    if (s?.isLeader && (!best || s.term > best.term)) best = s;
  }
  return best;
}

// "-" no data, "n/a" Prometheus unreachable, "err" Prometheus rejected the query.
export async function prom(base: string, q: string, f: Fetch = fetch): Promise<string> {
  try {
    const r = await f(`${base}/api/v1/query?query=${encodeURIComponent(q)}`, { cache: "no-store", signal: AbortSignal.timeout(3000) });
    const body = await r.json();
    if (body.status === "error") return "err";
    if (!r.ok) return "n/a";
    const v = body.data?.result?.[0]?.value?.[1];
    return v === undefined || v === "NaN" ? "-" : String(Math.round(Number(v) * 1000) / 1000);
  } catch {
    return "n/a";
  }
}
