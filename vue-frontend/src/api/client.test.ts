import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, request } from "./client";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("typed API client", () => {
  it("uses same-origin credentials and decodes a successful response", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({ required: true, configured: true, authenticated: true }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const status = await api.authStatus();

    expect(status.authenticated).toBe(true);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/auth/status",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("preserves a backend detail message and HTTP status", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "state reconciliation is pending" }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );

    try {
      await request("/api/grid/status");
      expect.fail("request should reject");
    } catch (reason) {
      expect(reason).toBeInstanceOf(ApiError);
      const error = reason as ApiError;
      expect(error.message).toBe("state reconciliation is pending");
      expect(error.status).toBe(503);
    }
  });

  it("joins FastAPI validation messages instead of hiding them", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: [{ msg: "upper price invalid" }, { msg: "qty invalid" }] }),
          { status: 422, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(request("/api/grid/preview")).rejects.toThrow(
      "upper price invalid; qty invalid",
    );
  });
});
