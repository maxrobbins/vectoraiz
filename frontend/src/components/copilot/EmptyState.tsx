import { Globe, MessageCircle, ExternalLink } from "lucide-react";
import { useBrand } from "@/contexts/BrandContext";

interface EmptyStateProps {
  isStandalone: boolean;
  allieAvailable: boolean;
}

export default function EmptyState({ isStandalone, allieAvailable }: EmptyStateProps) {
  const { docsConnectedModeUrl } = useBrand();

  if (isStandalone || !allieAvailable) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center px-6 text-center gap-4">
        <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center">
          <Globe className="h-6 w-6 text-muted-foreground" />
        </div>
        <div className="space-y-1.5">
          <h3 className="font-medium text-sm text-foreground">
            allAI requires an ai.market connection to work.
          </h3>
          <p className="text-xs text-muted-foreground leading-relaxed">
            Switch to connected mode to chat with allAI.
          </p>
        </div>
        <a
          href={docsConnectedModeUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
        >
          Learn More <ExternalLink className="h-3 w-3" />
        </a>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col items-center justify-center px-6 text-center gap-3">
      <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center">
        <MessageCircle className="h-6 w-6 text-primary" />
      </div>
      <div>
        <h3 className="font-medium text-sm text-foreground">Chat with Allie</h3>
        <p className="text-xs text-muted-foreground mt-1">
          Ask questions about your data, run queries, and get insights.
        </p>
      </div>
    </div>
  );
}
