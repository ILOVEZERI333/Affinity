import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import fixture from "../testdata/status.json";
import Page from "./page";

// Refresh is a client component that needs the Next router.
vi.mock("./refresh", () => ({ default: () => null }));

const render = async () => renderToStaticMarkup(await Page());

const json = (body: unknown, status = 200) => ({ ok: status < 400, status, json: async () => body });

// Routes the stubbed fetch by URL; an unmapped URL fails like an unreachable host.
function stubFetch(routes: Record<string, unknown>) {
  vi.stubGlobal("fetch", async (url: string) => {
    for (const [prefix, res] of Object.entries(routes)) {
      if (String(url).startsWith(prefix)) return res;
    }
    throw new Error("connection refused");
  });
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("Page", () => {
  it("shows the setup hint when WORKER_STATUS_URLS is empty", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "");
    stubFetch({});
    expect(await render()).toContain("Set WORKER_STATUS_URLS");
  });

  it("omits the hint once URLs are configured", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "http://w-0/status");
    stubFetch({ "http://w-0/": json(fixture) });
    expect(await render()).not.toContain("Set WORKER_STATUS_URLS");
  });

  it("renders an error row for a node that fails, alongside the healthy ones", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "http://w-0/status,http://w-1/status,http://w-2/status");
    stubFetch({ "http://w-0/": json(fixture), "http://w-1/": json({}, 503) });
    const html = await render();
    expect(html).toContain("❌ http://w-1/status: HTTP 503");
    expect(html).toContain("❌ http://w-2/status: Error: connection refused");
    expect(html).toContain("<td>w-0</td><td>leader</td>");
  });

  it("names the leader with the highest term when two nodes claim it", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "http://w-0/status,http://w-2/status");
    stubFetch({
      "http://w-0/": json({ ...fixture, node: "w-0", isLeader: true, term: 2 }),
      "http://w-2/": json({ ...fixture, node: "w-2", isLeader: true, term: 3 }),
    });
    expect(await render()).toContain("Leader: <b>w-2</b>");
  });

  it("says no leader is reachable when every node is down", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "http://w-0/status");
    stubFetch({});
    expect(await render()).toContain("none reachable");
  });

  it("shows err for a stat tile when Prometheus rejects the query", async () => {
    vi.stubEnv("WORKER_STATUS_URLS", "http://w-0/status");
    vi.stubEnv("PROMETHEUS_URL", "http://prom");
    stubFetch({ "http://w-0/": json(fixture), "http://prom/": json({ status: "error", error: "bad" }, 400) });
    expect(await render()).toContain('<div style="font-size:24px">err</div>');
  });
});
