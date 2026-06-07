// SSE frame parsing and event-type extraction.
import { describe, expect, it } from "vitest";

import { eventType, parseSseFrame } from "../../src/api/sse";

describe("parseSseFrame", () => {
  it("parses a JSON data frame", () => {
    const event = parseSseFrame('data: {"type":"thinking.delta","content":"hi"}');
    expect(event).toBeDefined();
    expect(event?.data).toEqual({ type: "thinking.delta", content: "hi" });
  });

  it("keeps plain-text data as a string", () => {
    const event = parseSseFrame("data: not json");
    expect(event?.data).toBe("not json");
    expect(event?.raw).toBe("not json");
  });

  it("joins multi-line data fields", () => {
    const event = parseSseFrame("data: line1\ndata: line2");
    expect(event?.raw).toBe("line1\nline2");
  });

  it("captures event and id fields", () => {
    const event = parseSseFrame('event: update\nid: 42\ndata: {"x":1}');
    expect(event?.event).toBe("update");
    expect(event?.id).toBe("42");
  });

  it("ignores heartbeat comments", () => {
    expect(parseSseFrame(": keep-alive")).toBeUndefined();
  });

  it("returns undefined for empty frames", () => {
    expect(parseSseFrame("")).toBeUndefined();
  });

  it("strips a single leading space after the colon only", () => {
    const event = parseSseFrame("data:  two-spaces");
    expect(event?.raw).toBe(" two-spaces");
  });
});

describe("eventType", () => {
  it("prefers the SSE event field", () => {
    const event = parseSseFrame('event: custom\ndata: {"type":"inner"}');
    expect(event && eventType(event)).toBe("custom");
  });

  it("falls back to the JSON type marker", () => {
    const event = parseSseFrame('data: {"type":"job.done"}');
    expect(event && eventType(event)).toBe("job.done");
  });

  it("returns undefined when neither is present", () => {
    const event = parseSseFrame("data: plain");
    expect(event && eventType(event)).toBeUndefined();
  });
});
