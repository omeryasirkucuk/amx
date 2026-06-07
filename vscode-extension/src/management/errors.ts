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
