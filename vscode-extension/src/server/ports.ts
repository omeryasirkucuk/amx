// Free-port probing for the Studio server spawn.
import { createServer } from "node:net";

/** Resolve true when the port is free to bind on 127.0.0.1. */
export function isPortFree(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const probe = createServer();
    probe.unref();
    probe.once("error", () => resolve(false));
    probe.listen({ port, host: "127.0.0.1", exclusive: true }, () => {
      probe.close(() => resolve(true));
    });
  });
}

/** Resolve an ephemeral free port assigned by the OS. */
export function ephemeralPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.unref();
    probe.once("error", reject);
    probe.listen({ port: 0, host: "127.0.0.1", exclusive: true }, () => {
      const address = probe.address();
      if (address === null || typeof address === "string") {
        probe.close(() => reject(new Error("could not determine ephemeral port")));
        return;
      }
      const { port } = address;
      probe.close(() => resolve(port));
    });
  });
}

/** Preferred port when free, otherwise an OS-assigned ephemeral one. */
export async function pickPort(preferred: number): Promise<number> {
  if (preferred > 0 && (await isPortFree(preferred))) return preferred;
  return ephemeralPort();
}
