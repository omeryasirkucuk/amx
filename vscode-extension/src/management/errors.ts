// Shared error-handling helpers for management commands.
// Both helpers show a branded AMX error toast and swallow the exception so
// callers can bail out cleanly without try/catch boilerplate.
import * as vscode from "vscode";

/**
 * Run `body`, catch any thrown error, and show
 * "AMX: could not <action>: <message>" as an error notification.
 */
export async function guard(action: string, body: () => Promise<void>): Promise<void> {
  try {
    await body();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not ${action}: ${message}`);
  }
}

/**
 * Run `body`; on failure show "AMX: could not <action>: <message>" with a
 * Retry button and re-run `body` when Retry is clicked. Loops until the
 * body succeeds or the user dismisses the notification. Intended for
 * SUBMIT steps after a wizard, so the collected answers are not lost to
 * a transient server error.
 */
export async function guardWithRetry(action: string, body: () => Promise<void>): Promise<void> {
  for (;;) {
    try {
      await body();
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const choice = await vscode.window.showErrorMessage(
        `AMX: could not ${action}: ${message}`,
        "Retry",
      );
      if (choice !== "Retry") return;
    }
  }
}

/**
 * Run `body` and return its value. On error show
 * "AMX: could not <action>: <message>" and return `undefined`.
 */
export async function guardValue<T>(action: string, body: () => Promise<T>): Promise<T | undefined> {
  try {
    return await body();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not ${action}: ${message}`);
    return undefined;
  }
}
