// Per-document scan cache. Scans are keyed by (uri, version) so each
// provider invocation on an unchanged document reuses the same
// ScanResult; oversized documents get an empty result. Also exposes
// a 500ms-debounced change event the diagnostics provider consumes.
import * as vscode from "vscode";

import { scanSql } from "./scanner";
import type { ScanResult } from "./types";

const CHANGE_DEBOUNCE_MS = 500;
const EMPTY_SCAN: ScanResult = { tokens: [], statements: [] };

export class DocumentStateStore implements vscode.Disposable {
  private readonly scans = new Map<string, { version: number; scan: ScanResult }>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly changeEmitter = new vscode.EventEmitter<vscode.TextDocument>();
  private readonly subscriptions: vscode.Disposable[];

  /** Fires at most once per 500ms per document after edits settle. */
  readonly onDidChangeScan = this.changeEmitter.event;

  constructor() {
    this.subscriptions = [
      vscode.workspace.onDidChangeTextDocument((event) => this.scheduleChange(event.document)),
      vscode.workspace.onDidCloseTextDocument((document) => this.forget(document)),
    ];
  }

  /** Cached scan for the document's current version. */
  getScan(document: vscode.TextDocument): ScanResult {
    const key = document.uri.toString();
    const cached = this.scans.get(key);
    if (cached && cached.version === document.version) return cached.scan;
    const maxKb = vscode.workspace
      .getConfiguration("amx.editor")
      .get<number>("maxFileSizeKb", 512);
    const text = document.getText();
    const scan = text.length > maxKb * 1024 ? EMPTY_SCAN : scanSql(text);
    this.scans.set(key, { version: document.version, scan });
    return scan;
  }

  dispose(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    this.scans.clear();
    this.changeEmitter.dispose();
    for (const subscription of this.subscriptions) subscription.dispose();
  }

  private scheduleChange(document: vscode.TextDocument): void {
    const key = document.uri.toString();
    const existing = this.timers.get(key);
    if (existing) clearTimeout(existing);
    this.timers.set(
      key,
      setTimeout(() => {
        this.timers.delete(key);
        this.changeEmitter.fire(document);
      }, CHANGE_DEBOUNCE_MS),
    );
  }

  private forget(document: vscode.TextDocument): void {
    const key = document.uri.toString();
    this.scans.delete(key);
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }
}
