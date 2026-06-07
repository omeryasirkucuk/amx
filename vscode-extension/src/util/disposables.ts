// Minimal disposable store. Mirrors vscode.Disposable semantics
// without importing vscode so service-layer classes stay testable.

export interface DisposableLike {
  dispose(): unknown;
}

export class DisposableStore implements DisposableLike {
  private readonly items: DisposableLike[] = [];
  private disposed = false;

  add<T extends DisposableLike>(item: T): T {
    if (this.disposed) {
      item.dispose();
    } else {
      this.items.push(item);
    }
    return item;
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    while (this.items.length) {
      try {
        this.items.pop()?.dispose();
      } catch {
        // Disposal must never throw past the store — keep unwinding.
      }
    }
  }
}
