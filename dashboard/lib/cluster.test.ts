import { describe, expect, it } from "vitest";
import { QUERIES, fetchNode, parseUrls, pickLeader, prom, type NodeResult, type NodeStatus } from "./cluster";
import fixture from "../testdata/status.json";

const resp = (body: unknown, init: { ok?: boolean; status?: number } = {}) =>
  (async () => ({ ok: init.ok ?? true, status: init.status ?? 200, json: async () => body })) as unknown as typeof fetch;
const boom = (async () => { throw new Error("connection refused"); }) as unknown as typeof fetch;

const node = (id: string, isLeader: boolean, term: number): NodeResult => ({
  url: id,
  s: { ...(fixture as NodeStatus), node: id, isLeader, term },
});

describe("parseUrls", () => {
  it("is empty when unset or blank", () => {
    expect(parseUrls(undefined)).toEqual([]);
    expect(parseUrls(" , ")).toEqual([]);
  });
  it("trims and splits", () => {
    expect(parseUrls("http://a:1/status, http://b:1/status")).toEqual(["http://a:1/status", "http://b:1/status"]);
  });
});

describe("pickLeader", () => {
  it("picks the leader claim with the highest term", () => {
    expect(pickLeader([node("w-0", true, 2), node("w-1", false, 3), node("w-2", true, 3)])?.node).toBe("w-2");
  });
  it("is undefined when nobody claims leadership or nobody answered", () => {
    expect(pickLeader([node("w-0", false, 1), { url: "x", err: "down" }])).toBeUndefined();
    expect(pickLeader([])).toBeUndefined();
  });
});

describe("fetchNode", () => {
  it("returns the parsed status", async () => {
    const r = await fetchNode("u", resp(fixture));
    expect(r.s?.node).toBe("w-0");
  });
  it("turns a non-2xx into an error row", async () => {
    expect(await fetchNode("u", resp({}, { ok: false, status: 503 }))).toEqual({ url: "u", err: "HTTP 503" });
  });
  it("turns a malformed body into an error row", async () => {
    for (const body of [null, {}, { ...fixture, owned: null }, { ...fixture, handoffs: {} }]) {
      expect(await fetchNode("u", resp(body))).toEqual({ url: "u", err: "malformed status response" });
    }
  });
  it("turns a network failure into an error row", async () => {
    const r = await fetchNode("u", boom);
    expect(r.s).toBeUndefined();
    expect(r.err).toContain("connection refused");
  });
});

describe("prom", () => {
  const ok = (v: unknown) => resp({ status: "success", data: { result: v === undefined ? [] : [{ value: [0, v] }] } });
  it("rounds a value to 3 decimals", async () => expect(await prom("p", "q", ok("1.23456"))).toBe("1.235"));
  it("shows - when there is no data or NaN", async () => {
    expect(await prom("p", "q", ok(undefined))).toBe("-");
    expect(await prom("p", "q", ok("NaN"))).toBe("-");
  });
  it("shows err when Prometheus rejects the query", async () => {
    expect(await prom("p", "q", resp({ status: "error", error: "bad" }, { ok: false, status: 400 }))).toBe("err");
  });
  it("shows n/a when Prometheus is unreachable or answers non-JSON-ish failures", async () => {
    expect(await prom("p", "q", boom)).toBe("n/a");
    expect(await prom("p", "q", resp({}, { ok: false, status: 502 }))).toBe("n/a");
  });
});

describe("QUERIES", () => {
  it("defines the four stat tiles", () => {
    expect(Object.keys(QUERIES)).toHaveLength(4);
  });
});
