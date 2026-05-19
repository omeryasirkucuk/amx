// New-page wizard: title + intent -> assets -> sources -> generate.
// Step 1 creates the page record so steps 2+ can attach to a real id.

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Check, Loader2 } from "lucide-react";

import PageHeader from "../components/PageHeader";
import AssetPicker from "../components/pages/AssetPicker";
import SourceAttacher from "../components/pages/SourceAttacher";
import { Button, Field, Input, Textarea, useToast } from "../components/ui";
import { cn } from "../lib/cn";
import {
  useCreatePage,
  useGeneratePage,
  type PageAssetRef,
  type PageSource,
} from "../hooks/usePages";

type Step = 1 | 2 | 3 | 4;

const STEP_LABELS: Record<Step, string> = {
  1: "Title & intent",
  2: "Assets",
  3: "Sources",
  4: "Generate",
};

export default function PageNewRoute() {
  const navigate = useNavigate();
  const toast = useToast();
  const create = useCreatePage();

  const [step, setStep] = useState<Step>(1);
  const [title, setTitle] = useState("");
  const [intent, setIntent] = useState("");
  const [assets, setAssets] = useState<PageAssetRef[]>([]);
  const [pageId, setPageId] = useState<string | null>(null);
  const [sources, setSources] = useState<PageSource[]>([]);

  const generate = useGeneratePage(pageId ?? "");

  async function submitStep1() {
    if (!title.trim()) {
      toast.push({ title: "Title is required", tone: "warning" });
      return;
    }
    try {
      const created = await create.mutateAsync({
        title: title.trim(),
        intent: intent.trim(),
        assets,
      });
      setPageId(created.id);
      setStep(2);
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    }
  }

  async function runGenerate() {
    if (!pageId) return;
    try {
      await generate.mutateAsync();
      toast.push({ title: "Page generated", tone: "success" });
      navigate(`/pages/${pageId}`);
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    }
  }

  return (
    <div className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6">
      <PageHeader
        title="New documentation page"
        breadcrumbs={[{ label: "Pages", to: "/pages" }, { label: "New" }]}
      />
      <Stepper current={step} />
      <div className="mt-6 space-y-4">
        {step === 1 && (
          <div className="space-y-4">
            <Field label="Title" required>
              <Input
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                placeholder="e.g. Sales mart overview"
                autoFocus
              />
            </Field>
            <Field
              label="Intent"
              hint="Optional. A short description of what this page should cover."
            >
              <Textarea
                value={intent}
                onChange={(e) => setIntent(e.target.value)}
                placeholder="What story should this page tell?"
                rows={4}
              />
            </Field>
            <Field label="Initial assets">
              <AssetPicker value={assets} onChange={setAssets} />
            </Field>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => navigate("/pages")}>
                Cancel
              </Button>
              <Button
                variant="primary"
                onClick={submitStep1}
                loading={create.isPending}
                disabled={!title.trim()}
              >
                Continue
              </Button>
            </div>
          </div>
        )}
        {step === 2 && pageId && (
          <div className="space-y-4">
            <Field label="Review assets">
              <AssetPicker value={assets} onChange={setAssets} />
              <p className="mt-2 text-[11px] text-ink-dim">
                Selection changes here are local. Re-open the page to persist
                additional assets after generation.
              </p>
            </Field>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => setStep(1)}>
                Back
              </Button>
              <Button variant="primary" onClick={() => setStep(3)}>
                Continue
              </Button>
            </div>
          </div>
        )}
        {step === 3 && pageId && (
          <div className="space-y-4">
            <Field
              label="Attach sources"
              hint="Optional. Upload supporting files (spreadsheets, emails, PDFs) to ground the generation."
            >
              <SourceAttacher
                pageId={pageId}
                sources={sources}
                onChange={setSources}
              />
            </Field>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => setStep(2)}>
                Back
              </Button>
              <Button variant="secondary" onClick={() => setStep(4)}>
                Skip
              </Button>
              <Button variant="primary" onClick={() => setStep(4)}>
                Continue
              </Button>
            </div>
          </div>
        )}
        {step === 4 && pageId && (
          <div className="space-y-4">
            <div className="rounded-md border border-border bg-surface p-4 text-sm text-ink">
              <div className="font-medium">Ready to generate</div>
              <p className="mt-1 text-ink-muted">
                Assets: {assets.length}. Sources: {sources.length}. This step
                consumes tokens on the active LLM.
              </p>
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => setStep(3)}>
                Back
              </Button>
              <Button
                variant="primary"
                onClick={runGenerate}
                loading={generate.isPending}
                leadingIcon={
                  generate.isPending ? (
                    <Loader2 size={14} className="animate-spin" />
                  ) : undefined
                }
              >
                Generate page
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function Stepper({ current }: { current: Step }) {
  const steps: Step[] = [1, 2, 3, 4];
  return (
    <>
      {/* Horizontal on md+ */}
      <ol className="hidden md:flex items-center gap-2">
        {steps.map((s, i) => {
          const done = current > s;
          const active = current === s;
          return (
            <li key={s} className="flex items-center gap-2">
              <div
                className={cn(
                  "inline-flex h-6 w-6 items-center justify-center rounded-full border text-xs font-semibold",
                  done && "border-accent bg-accent text-white",
                  active && "border-accent text-accent-ink",
                  !done && !active && "border-border text-ink-dim",
                )}
              >
                {done ? <Check size={12} /> : s}
              </div>
              <span
                className={cn(
                  "text-sm",
                  active ? "font-medium text-ink" : "text-ink-muted",
                )}
              >
                {STEP_LABELS[s]}
              </span>
              {i < steps.length - 1 && (
                <span className="h-px w-8 bg-border" aria-hidden="true" />
              )}
            </li>
          );
        })}
      </ol>
      {/* Vertical on sm */}
      <ol className="md:hidden space-y-1">
        {steps.map((s) => {
          const done = current > s;
          const active = current === s;
          return (
            <li key={s} className="flex items-center gap-2">
              <div
                className={cn(
                  "inline-flex h-5 w-5 items-center justify-center rounded-full border text-[10px] font-semibold",
                  done && "border-accent bg-accent text-white",
                  active && "border-accent text-accent-ink",
                  !done && !active && "border-border text-ink-dim",
                )}
              >
                {done ? <Check size={10} /> : s}
              </div>
              <span
                className={cn(
                  "text-xs",
                  active ? "font-medium text-ink" : "text-ink-muted",
                )}
              >
                {STEP_LABELS[s]}
              </span>
            </li>
          );
        })}
      </ol>
    </>
  );
}
