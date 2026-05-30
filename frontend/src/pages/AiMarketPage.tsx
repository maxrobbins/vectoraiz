import {
  CheckCircle,
  Info,
  Globe,
  ShoppingBag,
  Bot,
} from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useMode } from "@/contexts/ModeContext";
import { useCoPilot } from "@/contexts/CoPilotContext";
import { Progress } from "@/components/ui/progress";
import { Link } from "react-router-dom";

const AiMarketPage = () => {
  const { isStandalone, hasFeature } = useMode();
  const { allieAvailable, isStandalone: allieStandalone, connectionStatus } = useCoPilot();
  const isConnectedMode = !allieStandalone && allieAvailable;

  return (
    <div className="space-y-6 max-w-3xl pb-20">
      <div>
        <p className="text-muted-foreground">
          Manage your ai.market connection and services
        </p>
      </div>

      {/* Section A: Connection Status */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Globe className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Connection Status</CardTitle>
              <CardDescription>Your ai.market connection</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {!isStandalone ? (
            <div className="p-4 bg-[hsl(var(--haven-success))]/10 border border-[hsl(var(--haven-success))]/30 rounded-lg space-y-3">
              <div className="flex items-center gap-2">
                <CheckCircle className="w-5 h-5 text-[hsl(var(--haven-success))]" />
                <span className="text-sm font-medium text-[hsl(var(--haven-success))]">
                  Connected to ai.market
                </span>
              </div>
              <div className="grid grid-cols-2 gap-2 text-sm">
                <span className="text-muted-foreground">API Key</span>
                <span className="font-mono text-foreground">am_****...****</span>
                <span className="text-muted-foreground">Status</span>
                <span className="text-foreground capitalize">{connectionStatus}</span>
              </div>
            </div>
          ) : (
            <div className="p-4 bg-[hsl(var(--haven-warning))]/10 border border-[hsl(var(--haven-warning))]/30 rounded-lg space-y-3">
              <div className="flex items-center gap-2">
                <Info className="w-5 h-5 text-[hsl(var(--haven-warning))]" />
                <span className="text-sm font-medium text-[hsl(var(--haven-warning))]">
                  Not connected
                </span>
              </div>
              <p className="text-sm text-muted-foreground">
                Connect to ai.market to unlock allAI, premium document processing, and the data marketplace. Run:
              </p>
              <pre className="text-xs font-mono bg-background p-2 rounded border border-border">
                ./start.sh --setup-allie
              </pre>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Section B: allAI — AI Data Assistant */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Bot className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">allAI — AI Data Assistant</CardTitle>
              <CardDescription>AI-powered assistant for data exploration and queries</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {isConnectedMode ? (
            <>
              <div className="flex items-center gap-2 p-3 bg-[hsl(var(--haven-success))]/10 border border-[hsl(var(--haven-success))]/30 rounded-lg">
                <CheckCircle className="w-4 h-4 text-[hsl(var(--haven-success))] flex-shrink-0" />
                <span className="text-sm text-[hsl(var(--haven-success))]">
                  allAI is active — powered by ai.market
                </span>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Credit Usage</span>
                  <span className="text-foreground font-mono">—</span>
                </div>
                <Progress value={0} className="h-2" />
                <p className="text-xs text-muted-foreground">Credit usage data will appear here when available.</p>
              </div>
            </>
          ) : (
            <div className="flex items-center gap-2 p-3 bg-muted/50 border border-border rounded-lg">
              <Info className="w-4 h-4 text-muted-foreground flex-shrink-0" />
              <span className="text-sm text-muted-foreground">
                allAI requires an ai.market connection. Run <code className="text-xs bg-muted px-1 py-0.5 rounded">./start.sh --setup-allie</code> to enable.
              </span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Section C: Marketplace */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <ShoppingBag className="w-5 h-5 text-primary" />
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <CardTitle className="text-foreground">Marketplace</CardTitle>
                {(isStandalone || !hasFeature("marketplace")) && (
                  <span className="text-xs px-2 py-0.5 rounded-full bg-primary/10 text-primary border border-primary/20 font-medium">
                    Coming Soon
                  </span>
                )}
              </div>
              <CardDescription>
                Publish your datasets to the ai.market marketplace and earn revenue when AI systems purchase access
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {!isStandalone && hasFeature("marketplace") ? (
            <Link
              to="/earnings"
              className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
            >
              View Earnings Dashboard
              <span aria-hidden="true">&rarr;</span>
            </Link>
          ) : (
            <div className="flex items-center gap-2 p-3 bg-muted/50 border border-border rounded-lg">
              <Info className="w-4 h-4 text-muted-foreground flex-shrink-0" />
              <span className="text-sm text-muted-foreground">
                Connect to ai.market to access the data marketplace and start earning from your datasets.
              </span>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default AiMarketPage;
