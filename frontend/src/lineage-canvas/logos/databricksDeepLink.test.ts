import { describe, expect, it } from "vitest";
import { databricksDeepLink } from "./databricksDeepLink";

describe("databricksDeepLink", () => {
  const host = "example.cloud.databricks.com";
  it("builds a table link from fqn", () => {
    expect(databricksDeepLink({ kind: "table", host, fqn: "cat.sch.tbl" })).toBe(
      "https://example.cloud.databricks.com/explore/data/cat/sch/tbl",
    );
  });
  it("builds a notebook link from externalId (object_id hash route)", () => {
    expect(databricksDeepLink({ kind: "notebook", host, externalId: "123" })).toBe(
      "https://example.cloud.databricks.com/#notebook/123",
    );
  });
  it("builds a job link", () => {
    expect(databricksDeepLink({ kind: "job", host, externalId: "9" })).toBe(
      "https://example.cloud.databricks.com/jobs/9",
    );
  });
  it("normalizes a host that already has a scheme and trailing slash", () => {
    expect(
      databricksDeepLink({ kind: "job", host: "https://example.cloud.databricks.com/", externalId: "9" }),
    ).toBe("https://example.cloud.databricks.com/jobs/9");
  });
  it("returns null when host missing or identifier absent", () => {
    expect(databricksDeepLink({ kind: "job", host: "", externalId: "9" })).toBeNull();
    expect(databricksDeepLink({ kind: "notebook", host, externalId: undefined })).toBeNull();
    expect(databricksDeepLink({ kind: "dashboard", host, externalId: "1" })).toBeNull();
  });
});
