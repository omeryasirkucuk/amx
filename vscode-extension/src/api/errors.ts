// Error type for AMX Studio API failures. Carries the classified
// detail/hint JSON the backend attaches (see e.g. the comments
// router's _classified_or_400) so callers can surface actionable
// messages instead of bare status codes.

export class AmxApiError extends Error {
  constructor(
    readonly status: number,
    readonly detail: string,
    readonly hint?: string,
  ) {
    super(hint ? `${detail} (${hint})` : detail);
    this.name = "AmxApiError";
  }
}

/** Build an AmxApiError from a non-2xx response body. */
export async function errorFromResponse(response: Response): Promise<AmxApiError> {
  let detail = `${response.status} ${response.statusText}`;
  let hint: string | undefined;
  try {
    const body: unknown = await response.json();
    if (body && typeof body === "object") {
      const raw = (body as { detail?: unknown }).detail;
      if (typeof raw === "string") {
        detail = raw;
      } else if (raw && typeof raw === "object") {
        const classified = raw as { message?: unknown; hint?: unknown };
        if (typeof classified.message === "string") detail = classified.message;
        if (typeof classified.hint === "string") hint = classified.hint;
      }
    }
  } catch {
    // Non-JSON body — keep the status line.
  }
  return new AmxApiError(response.status, detail, hint);
}
