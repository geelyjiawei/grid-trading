import { afterAll, beforeAll } from "vitest";

// jsdom does not implement native dialog methods. Actual modality/focus trapping
// is verified in the browser; this stub only models the open/close lifecycle.
const prototype = HTMLDialogElement.prototype;
const previousShow = Object.getOwnPropertyDescriptor(prototype, "showModal");
const previousClose = Object.getOwnPropertyDescriptor(prototype, "close");
beforeAll(() => {
  Object.defineProperty(prototype, "showModal", {
    configurable: true,
    value(this: HTMLDialogElement) {
      this.setAttribute("open", "");
      this.querySelector<HTMLElement>("[autofocus]")?.focus();
    },
  });
  Object.defineProperty(prototype, "close", {
    configurable: true,
    value(this: HTMLDialogElement) {
      this.removeAttribute("open");
      this.dispatchEvent(new Event("close"));
    },
  });
});
afterAll(() => {
  if (previousShow) Object.defineProperty(prototype, "showModal", previousShow);
  else Reflect.deleteProperty(prototype, "showModal");
  if (previousClose) Object.defineProperty(prototype, "close", previousClose);
  else Reflect.deleteProperty(prototype, "close");
});
