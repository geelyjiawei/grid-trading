import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, request, withExchange } from "./client";

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

  it("surfaces the structured Rust API error message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: { code: "authentication_required", message: "Authentication required" },
          }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(request("/api/config")).rejects.toMatchObject({
      status: 401,
      message: "Authentication required",
    });
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

  it("adds an exchange selector without dropping an existing query", () => {
    expect(withExchange("/api/trades/MUUSDT?limit=100", "aster")).toBe(
      "/api/trades/MUUSDT?limit=100&exchange=aster",
    );
  });

  it("posts login credentials as JSON without putting them in the URL", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.login({ username: "admin", password: "temporary", code: "123456" });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/auth/login",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ username: "admin", password: "temporary", code: "123456" }),
      }),
    );
  });

  it("uses a same-origin POST to revoke the web session", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.logout();

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/auth/logout",
      expect.objectContaining({ method: "POST", credentials: "same-origin" }),
    );
  });
});
