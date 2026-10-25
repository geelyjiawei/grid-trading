import type { GridOrder } from "./api/types";

// Compare decimal strings without converting exchange prices to binary floats.
export function comparePrices(left: unknown, right: unknown): number {
  const parts = (value: unknown) => {
    const match = /^\+?(\d+)(?:\.(\d+))?$/.exec(String(value ?? "").trim());
    return match ? [match[1].replace(/^0+(?=\d)/, ""), match[2] ?? ""] : null;
  };
  const a = parts(left);
  const b = parts(right);
  if (!a || !b) return a ? -1 : b ? 1 : 0;
  if (a[0].length !== b[0].length) return a[0].length - b[0].length;
  if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
  const length = Math.max(a[1].length, b[1].length);
  const af = a[1].padEnd(length, "0");
  const bf = b[1].padEnd(length, "0");
  return af === bf ? 0 : af < bf ? -1 : 1;
}

export function orderStatusLabel(status?: string): string {
  const labels: Record<string, string> = {
    NEW: "待成交",
    OPEN: "待成交",
    PARTIALLY_FILLED: "部分成交",
    FILLED: "全部成交",
    CANCELED: "已撤单",
    CANCELLED: "已撤单",
    REJECTED: "已拒绝",
    EXPIRED: "已过期",
    EXPIRED_IN_MATCH: "撮合过期",
    PENDING_CANCEL: "撤单中",
  };
  return status ? (labels[status.toUpperCase()] ?? status) : "--";
}
export function orderIdentity(order: GridOrder): string {
  return (
    order.order_id ??
    order.orderId ??
    order.order_link_id ??
    order.orderLinkId ??
    `${order.side}:${order.price}:${order.qty}`
  );
}
export function timestampMillis(value: unknown): number {
  if (value == null || value === "") return 0;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return numeric < 1_000_000_000_000 ? numeric * 1000 : numeric;
}
