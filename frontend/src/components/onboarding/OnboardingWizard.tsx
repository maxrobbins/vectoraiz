/**
 * OnboardingWizard — First-run onboarding that branches on channel.
 *
 * Both channels see the same features (C2: presentation-only).
 * Only step ORDER and COPY differ.
 *
 * Shows once, gated by localStorage key "vz-onboarding-complete".
 *
 * BQ-VZ-CHANNEL Phase 2
 */
import { useState } from "react";
import {
  Check,
  ChevronRight,
  Database,
  Globe,
  Layers,
  Rocket,
  Search,
  ShoppingBag,
  Upload,
  Zap,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { type Channel } from "@/hooks/useChannel";

const STORAGE_KEY = "vz-onboarding-complete";

export function isOnboardingComplete(): boolean {
  try {
    return localStorage.getItem(STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

export function markOnboardingComplete(): void {
  try {
    localStorage.setItem(STORAGE_KEY, "true");
  } catch {
    // Storage unavailable — silently skip
  }
}

interface StepDef {
  title: string;
  subtitle: string;
  description: string;
  icon: React.ReactNode;
  features: string[];
}

const ICON_CLASS = "w-8 h-8";

const DIRECT_STEPS: StepDef[] = [
  {
    title: "Welcome to vectorAIz",
    subtitle: "Upload your data",
    description:
      "Get started by uploading CSV, JSON, or Parquet files. Your data stays on your machine — always.",
    icon: <Upload className={ICON_CLASS} />,
    features: ["Drag-and-drop upload", "CSV, JSON, Parquet support", "Local-only processing"],
  },
  {
    title: "Vectorize & Index",
    subtitle: "Choose embedding model",
    description:
      "Transform your data into searchable vectors. Pick an embedding model that fits your use case.",
    icon: <Layers className={ICON_CLASS} />,
    features: ["Multiple embedding models", "Automatic indexing", "BYO API key"],
  },
  {
    title: "Query your data",
    subtitle: "Try a RAG query",
    description:
      "Search with natural language. Combine vector search with SQL for powerful data exploration.",
    icon: <Search className={ICON_CLASS} />,
    features: ["Natural language search", "SQL queries", "RAG with your LLM"],
  },
  {
    title: "Ready!",
    subtitle: "You're all set",
    description:
      "Explore your data, run queries, and build insights. You can also list datasets on ai.market when you're ready.",
    icon: <Rocket className={ICON_CLASS} />,
    features: ["Full-text & vector search", "Dashboard analytics", "ai.market publishing"],
  },
];

const MARKETPLACE_STEPS: StepDef[] = [
  {
    title: "Welcome to vectorAIz for ai.market",
    subtitle: "Connect to your ai.market account",
    description:
      "Your vectorAIz instance is linked to ai.market. Manage your data and publish listings directly.",
    icon: <Globe className={ICON_CLASS} />,
    features: ["ai.market integration", "Secure device linking", "Local-only processing"],
  },
  {
    title: "Upload your data",
    subtitle: "Add your dataset",
    description:
      "Upload CSV, JSON, or Parquet files. Your data stays on your machine until you choose to publish.",
    icon: <Database className={ICON_CLASS} />,
    features: ["Drag-and-drop upload", "CSV, JSON, Parquet support", "Local-only storage"],
  },
  {
    title: "Enhance & Preview",
    subtitle: "Review metadata and set pricing",
    description:
      "Enrich your dataset with tags and descriptions. Preview how it will appear on ai.market.",
    icon: <Zap className={ICON_CLASS} />,
    features: ["AI-powered descriptions", "Privacy scanning", "Pricing suggestions"],
  },
  {
    title: "Publish!",
    subtitle: "List on ai.market",
    description:
      "Publish your dataset to ai.market and start earning. Manage listings from your dashboard.",
    icon: <ShoppingBag className={ICON_CLASS} />,
    features: ["One-click publish", "Revenue tracking", "Listing management"],
  },
];

/** Exported for tests: get steps for a given channel */
export function getStepsForChannel(channel: Channel): StepDef[] {
  return channel === "marketplace" ? MARKETPLACE_STEPS : DIRECT_STEPS;
}

interface OnboardingWizardProps {
  channel: Channel;
  onComplete: () => void;
}

export default function OnboardingWizard({ channel, onComplete }: OnboardingWizardProps) {
  const [step, setStep] = useState(0);
  const steps = getStepsForChannel(channel);
  const current = steps[step];
  const isLast = step === steps.length - 1;

  const handleNext = () => {
    if (isLast) {
      markOnboardingComplete();
      onComplete();
    } else {
      setStep((s) => s + 1);
    }
  };

  const handleSkip = () => {
    markOnboardingComplete();
    onComplete();
  };

  return (
    <Dialog open onOpenChange={() => handleSkip()}>
      <DialogContent className="max-w-lg p-0 overflow-hidden">
        {/* Step indicator */}
        <div className="flex items-center justify-center gap-2 pt-6 px-6">
          {steps.map((_, i) => (
            <div key={i} className="flex items-center">
              <div
                className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${
                  step === i
                    ? "bg-primary text-primary-foreground"
                    : step > i
                    ? "bg-[hsl(var(--haven-success))] text-white"
                    : "bg-secondary text-muted-foreground"
                }`}
              >
                {step > i ? <Check className="w-4 h-4" /> : i + 1}
              </div>
              {i < steps.length - 1 && (
                <div
                  className={`w-8 h-0.5 mx-1 ${
                    step > i ? "bg-[hsl(var(--haven-success))]" : "bg-secondary"
                  }`}
                />
              )}
            </div>
          ))}
        </div>

        {/* Content */}
        <div className="px-6 pb-6 pt-4 text-center">
          <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center mx-auto mb-4 text-primary">
            {current.icon}
          </div>
          <h2 className="text-xl font-semibold text-foreground mb-1">{current.title}</h2>
          <p className="text-sm text-muted-foreground mb-4">{current.subtitle}</p>
          <p className="text-sm text-foreground/80 mb-6">{current.description}</p>

          {/* Feature pills */}
          <div className="flex flex-wrap justify-center gap-2 mb-6">
            {current.features.map((f) => (
              <span
                key={f}
                className="px-3 py-1 text-xs rounded-full bg-secondary text-muted-foreground"
              >
                {f}
              </span>
            ))}
          </div>

          {/* Actions */}
          <div className="flex items-center justify-between">
            <Button variant="ghost" size="sm" onClick={handleSkip}>
              Skip
            </Button>
            <Button onClick={handleNext} className="gap-2">
              {isLast ? "Get Started" : "Next"}
              {!isLast && <ChevronRight className="w-4 h-4" />}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
