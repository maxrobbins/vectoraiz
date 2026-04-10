import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Loader2,
  ExternalLink,
  ChevronDown,
  CreditCard,
  AlertTriangle,
  KeyRound,
  Mail,
  CheckCircle2,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { useBrand } from "@/contexts/BrandContext";
import { toast } from "@/hooks/use-toast";
import { getApiUrl } from "@/lib/api";

interface AllaiCredits {
  balance_usd: number;
  total_usd: number;
  used_usd: number;
  usage: Array<{
    model: string;
    input_tokens: number;
    output_tokens: number;
    cost_usd: number;
    created_at: string;
  }>;
}

interface AutoReloadConfig {
  enabled: boolean;
  threshold_usd: number;
  reload_amount_usd: number;
}

const BillingPage = () => {
  const brand = useBrand();
  const [searchParams, setSearchParams] = useSearchParams();

  // Credits state
  const [credits, setCredits] = useState<AllaiCredits | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [purchasing, setPurchasing] = useState(false);
  const [showPurchaseForm, setShowPurchaseForm] = useState(false);
  const [purchaseAmount, setPurchaseAmount] = useState("25");
  const [purchaseError, setPurchaseError] = useState<string | null>(null);

  // Auto-reload state
  const [autoReload, setAutoReload] = useState<AutoReloadConfig>({
    enabled: false,
    threshold_usd: 5,
    reload_amount_usd: 25,
  });
  const [autoReloadLoading, setAutoReloadLoading] = useState(false);
  const [editingAutoReload, setEditingAutoReload] = useState(false);
  const [editThreshold, setEditThreshold] = useState("5");

  // Auto-reload pending
  const [pendingReload, setPendingReload] = useState<{ pending: boolean; checkout_url?: string } | null>(null);

  // Account / recovery state
  const [accountInfo, setAccountInfo] = useState<{
    has_account: boolean;
    email_masked?: string;
    balance_cents?: number;
  } | null>(null);
  const [showRecoveryForm, setShowRecoveryForm] = useState(false);
  const [recoveryEmail, setRecoveryEmail] = useState("");
  const [recoverySending, setRecoverySending] = useState(false);
  const [recoveryEmailSent, setRecoveryEmailSent] = useState(false);
  const [verifyingMagicLink, setVerifyingMagicLink] = useState(false);

  // Usage history
  const [showUsageHistory, setShowUsageHistory] = useState(false);

  const apiKey = localStorage.getItem("vectoraiz_api_key");

  const fetchCredits = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (!apiKey) { setLoading(false); return; }
      const res = await fetch(`${getApiUrl()}/api/allai/credits`, {
        headers: { "X-API-Key": apiKey },
      });
      if (res.ok) {
        const data = await res.json();
        setCredits({
          balance_usd: data.balance_usd ?? 0,
          total_usd: data.credit_usd ?? 0,
          used_usd: data.used_usd ?? 0,
          usage: data.usage ?? [],
        });
      } else if (res.status === 409) {
        setError("not_connected");
      } else {
        const err = await res.json().catch(() => ({ detail: "Failed to fetch credits" }));
        setError(err.detail || "Failed to fetch credits");
      }
    } catch {
      setError("Failed to connect to server");
    } finally {
      setLoading(false);
    }
  }, [apiKey]);

  const fetchAutoReload = useCallback(async () => {
    try {
      if (!apiKey) return;
      const res = await fetch(`${getApiUrl()}/api/allai/credits/auto-reload`, {
        headers: { "X-API-Key": apiKey },
      });
      if (res.ok) {
        const data: AutoReloadConfig = await res.json();
        setAutoReload(data);
        setEditThreshold(String(data.threshold_usd));
      }
    } catch {
      // Silently fail — auto-reload is optional
    }
  }, [apiKey]);

  const saveAutoReload = async (config: AutoReloadConfig) => {
    setAutoReloadLoading(true);
    try {
      if (!apiKey) return;
      const res = await fetch(`${getApiUrl()}/api/allai/credits/auto-reload`, {
        method: "POST",
        headers: { "X-API-Key": apiKey, "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      if (res.ok) {
        const data: AutoReloadConfig = await res.json();
        setAutoReload(data);
        setEditingAutoReload(false);
        toast({ title: "Auto-reload saved" });
      } else {
        const err = await res.json().catch(() => ({ detail: "Failed to save" }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to save auto-reload config", variant: "destructive" });
    } finally {
      setAutoReloadLoading(false);
    }
  };

  const fetchPendingReload = useCallback(async () => {
    try {
      if (!apiKey) return;
      const res = await fetch(`${getApiUrl()}/api/allai/credits/auto-reload/pending`, {
        headers: { "X-API-Key": apiKey },
      });
      if (res.ok) {
        const data = await res.json();
        setPendingReload(data);
      }
    } catch {
      // Silently fail
    }
  }, [apiKey]);

  const fetchAccount = useCallback(async () => {
    try {
      if (!apiKey) return;
      const res = await fetch(`${getApiUrl()}/api/allai/account`, {
        headers: { "X-API-Key": apiKey },
      });
      if (res.ok) {
        setAccountInfo(await res.json());
      }
    } catch {
      // Silently fail — account check is optional
    }
  }, [apiKey]);

  const handleSendMagicLink = async () => {
    if (!apiKey || !recoveryEmail.trim()) return;
    setRecoverySending(true);
    try {
      const res = await fetch(`${getApiUrl()}/api/allai/auth/magic-link`, {
        method: "POST",
        headers: { "X-API-Key": apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ email: recoveryEmail.trim() }),
      });
      if (res.ok) {
        setRecoveryEmailSent(true);
      } else {
        const err = await res.json().catch(() => ({ detail: "Failed to send magic link" }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to send magic link", variant: "destructive" });
    } finally {
      setRecoverySending(false);
    }
  };

  const handleVerifyMagicLink = useCallback(async (token: string) => {
    if (!apiKey) return;
    setVerifyingMagicLink(true);
    try {
      const res = await fetch(`${getApiUrl()}/api/allai/auth/verify-magic-link`, {
        method: "POST",
        headers: { "X-API-Key": apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ token }),
      });
      if (res.ok) {
        const data = await res.json();
        const balanceStr = data.balance_cents != null
          ? `$${(data.balance_cents / 100).toFixed(2)} available.`
          : "";
        toast({ title: `Balance restored! ${balanceStr}` });
        fetchCredits();
        fetchAccount();
      } else {
        toast({
          title: "Error",
          description: "Magic link expired or already used. Please try again.",
          variant: "destructive",
        });
      }
    } catch {
      toast({
        title: "Error",
        description: "Magic link expired or already used. Please try again.",
        variant: "destructive",
      });
    } finally {
      setVerifyingMagicLink(false);
    }
  }, [apiKey, fetchCredits, fetchAccount]);

  const handlePurchase = async (amountUsd: number) => {
    setPurchasing(true);
    try {
      if (!apiKey) return;
      const res = await fetch(`${getApiUrl()}/api/allai/credits/purchase`, {
        method: "POST",
        headers: { "X-API-Key": apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ amount_usd: amountUsd }),
      });
      if (res.ok) {
        const data = await res.json();
        if (data.checkout_url) {
          window.open(data.checkout_url, "_blank");
          const onFocus = () => {
            fetchCredits();
            window.removeEventListener("focus", onFocus);
          };
          window.addEventListener("focus", onFocus);
        }
      } else {
        const err = await res.json().catch(() => ({ detail: "Failed to create checkout" }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to initiate purchase", variant: "destructive" });
    } finally {
      setPurchasing(false);
      setShowPurchaseForm(false);
    }
  };

  useEffect(() => {
    fetchCredits();
    fetchAutoReload();
    fetchPendingReload();
    fetchAccount();

    // Handle magic link token callback
    const magicLinkToken = searchParams.get("magic_link_token");
    if (magicLinkToken) {
      handleVerifyMagicLink(magicLinkToken);
      searchParams.delete("magic_link_token");
      setSearchParams(searchParams, { replace: true });
    }

    // Handle Stripe redirect params
    if (searchParams.get("credits") === "success") {
      toast({ title: "Credits added successfully!", description: "Your allAI credits have been added." });
      // Clear pending auto-reload since purchase completed
      if (apiKey) {
        fetch(`${getApiUrl()}/api/allai/credits/auto-reload/pending`, {
          method: "DELETE",
          headers: { "X-API-Key": apiKey },
        }).then(() => setPendingReload(null)).catch(() => {});
      }
      searchParams.delete("credits");
      setSearchParams(searchParams, { replace: true });
    } else if (searchParams.get("credits") === "cancelled") {
      toast({ title: "Purchase cancelled", description: "No credits were charged." });
      searchParams.delete("credits");
      setSearchParams(searchParams, { replace: true });
    }
  }, [fetchCredits, fetchAutoReload, fetchPendingReload, fetchAccount, handleVerifyMagicLink, searchParams, setSearchParams]);

  const formatTimeAgo = (dateStr: string) => {
    const diff = Date.now() - new Date(dateStr).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
  };

  const formatModel = (model: string) =>
    model
      .replace(/^claude-/, "")
      .replace(/-\d{8}$/, "")
      .replace(/(\w+)-(\d+)(?:-(\d+))?/, (_m, name, major, minor) =>
        `${name.charAt(0).toUpperCase() + name.slice(1)} ${major}${minor ? `.${minor}` : ""}`
      );

  return (
    <div className="space-y-6 max-w-3xl pb-20">
      <div>
        <h1 className="text-2xl font-semibold text-foreground">Billing</h1>
        <p className="text-muted-foreground">Manage your allAI credits and payment settings</p>
      </div>

      {/* Low Balance Banner */}
      {pendingReload?.pending && pendingReload.checkout_url && (
        <div
          className="flex items-center gap-3 rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 cursor-pointer hover:bg-yellow-500/15 transition-colors"
          onClick={() => window.open(pendingReload.checkout_url, "_blank")}
        >
          <AlertTriangle className="w-5 h-5 text-yellow-500 shrink-0" />
          <div className="flex-1">
            <p className="text-sm font-medium text-yellow-200">Your allAI credit balance is low.</p>
            <p className="text-xs text-yellow-300/70">Click here to reload your credits.</p>
          </div>
          <ExternalLink className="w-4 h-4 text-yellow-500 shrink-0" />
        </div>
      )}

      {/* Recovery Banner */}
      {accountInfo?.has_account === false && !verifyingMagicLink && (
        <div className="rounded-lg border border-indigo-500/30 bg-indigo-500/10 px-4 py-3">
          {!showRecoveryForm && !recoveryEmailSent ? (
            <button
              onClick={() => setShowRecoveryForm(true)}
              className="flex items-center gap-3 w-full text-left hover:opacity-90 transition-opacity"
            >
              <KeyRound className="w-5 h-5 text-indigo-400 shrink-0" />
              <div className="flex-1">
                <p className="text-sm font-medium text-indigo-200">
                  Already purchased credits? <span className="font-semibold">Recover your balance</span>
                </p>
                <p className="text-xs text-indigo-300/70">
                  If you reinstalled {brand.name}, sign in to restore your credit balance.
                </p>
              </div>
            </button>
          ) : recoveryEmailSent ? (
            <div className="flex items-start gap-3">
              <CheckCircle2 className="w-5 h-5 text-indigo-400 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-indigo-200">Check your email</p>
                <p className="text-xs text-indigo-300/70 mt-1">
                  We sent a sign-in link to {recoveryEmail}. Click the link in your email to restore your balance. The link expires in 15 minutes.
                </p>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center gap-3">
                <Mail className="w-5 h-5 text-indigo-400 shrink-0" />
                <p className="text-sm font-medium text-indigo-200">Enter your email to recover your balance</p>
              </div>
              <div className="flex items-center gap-2">
                <Input
                  type="email"
                  placeholder="you@example.com"
                  value={recoveryEmail}
                  onChange={(e) => setRecoveryEmail(e.target.value)}
                  className="flex-1"
                  onKeyDown={(e) => { if (e.key === "Enter") handleSendMagicLink(); }}
                />
                <Button
                  size="sm"
                  disabled={recoverySending || !recoveryEmail.trim()}
                  onClick={handleSendMagicLink}
                >
                  {recoverySending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : "Send magic link"}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setShowRecoveryForm(false);
                    setRecoveryEmail("");
                  }}
                >
                  Cancel
                </Button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Verifying magic link spinner */}
      {verifyingMagicLink && (
        <div className="flex items-center gap-2 text-indigo-300 text-sm px-1">
          <Loader2 className="w-4 h-4 animate-spin" />
          Verifying magic link...
        </div>
      )}

      {/* Balance Card */}
      <Card className="bg-gradient-to-br from-zinc-900 to-zinc-800 border-zinc-700">
        <CardContent className="pt-6 pb-6">
          {loading ? (
            <div className="flex items-center gap-2 text-zinc-400">
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading balance...
            </div>
          ) : error ? (
            <div className="space-y-2">
              <p className="text-zinc-400 text-sm">
                {error === "not_connected"
                  ? "Connect to ai.market in Settings to view your balance."
                  : error}
              </p>
            </div>
          ) : credits ? (
            <div className="space-y-4">
              <div>
                <p className="text-zinc-400 text-sm font-medium uppercase tracking-wider">Remaining Balance</p>
                <p className="text-4xl font-bold text-white mt-1">
                  ${credits.balance_usd.toFixed(2)}
                </p>
                <p className="text-zinc-400 text-sm mt-1">
                  ${credits.used_usd.toFixed(2)} used this period
                </p>
              </div>
              <Progress
                value={credits.total_usd > 0 ? (credits.balance_usd / credits.total_usd) * 100 : 0}
                className="h-2 bg-zinc-700"
              />
            </div>
          ) : (
            <p className="text-zinc-400 text-sm">No credit information available.</p>
          )}
        </CardContent>
      </Card>

      {/* Payment & Purchase */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                <CreditCard className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="font-medium text-foreground">Payment Method</p>
                <p className="text-sm text-muted-foreground">Managed by Stripe</p>
              </div>
            </div>
            <Button
              onClick={() => setShowPurchaseForm(!showPurchaseForm)}
              disabled={purchasing || !!error}
            >
              <ExternalLink className="w-3.5 h-3.5 mr-1.5" />
              Buy Credits
            </Button>
          </div>
          {showPurchaseForm && (
            <div className="space-y-3 pt-4 border-t border-border">
              <p className="text-sm text-muted-foreground">Credits are sold in units of $25</p>
              <div className="space-y-2">
                <Label htmlFor="purchase-amount" className="text-foreground">Amount (USD)</Label>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">$</span>
                  <Input
                    id="purchase-amount"
                    type="number"
                    min="25"
                    step="25"
                    value={purchaseAmount}
                    onChange={(e) => {
                      setPurchaseAmount(e.target.value);
                      setPurchaseError(null);
                    }}
                    className="w-32"
                  />
                </div>
                {purchaseError && (
                  <p className="text-sm text-destructive">{purchaseError}</p>
                )}
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  disabled={purchasing}
                  onClick={() => {
                    const amt = parseFloat(purchaseAmount);
                    if (isNaN(amt) || amt <= 0 || amt % 25 !== 0) {
                      setPurchaseError("Amount must be a positive multiple of $25");
                      return;
                    }
                    handlePurchase(amt);
                  }}
                >
                  {purchasing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : "Purchase"}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setShowPurchaseForm(false);
                    setPurchaseError(null);
                    setPurchaseAmount("25");
                  }}
                >
                  Cancel
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Auto-Reload Config */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium text-foreground">Auto-Reload</p>
              <p className="text-sm text-muted-foreground">
                {autoReload.enabled
                  ? `Reload to $${autoReload.reload_amount_usd} when balance reaches $${autoReload.threshold_usd}`
                  : "Automatically add credits when your balance is low"}
              </p>
            </div>
            <div className="flex items-center gap-3">
              <Switch
                checked={autoReload.enabled}
                onCheckedChange={(checked) => {
                  const updated = { ...autoReload, enabled: checked };
                  setAutoReload(updated);
                  saveAutoReload(updated);
                }}
                disabled={!!error}
              />
              {autoReload.enabled && !editingAutoReload && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setEditingAutoReload(true)}
                >
                  Edit
                </Button>
              )}
            </div>
          </div>

          {editingAutoReload && autoReload.enabled && (
            <div className="space-y-3 pt-2 border-t border-border">
              <div className="space-y-2">
                <Label htmlFor="threshold" className="text-foreground">
                  Reload when balance drops below
                </Label>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">$</span>
                  <Input
                    id="threshold"
                    type="number"
                    min="1"
                    step="1"
                    value={editThreshold}
                    onChange={(e) => setEditThreshold(e.target.value)}
                    className="w-24"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label className="text-foreground">Reload amount</Label>
                <p className="text-sm text-muted-foreground">$25.00 (fixed)</p>
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  disabled={autoReloadLoading}
                  onClick={() => {
                    const threshold = parseFloat(editThreshold);
                    if (isNaN(threshold) || threshold < 1) {
                      toast({ title: "Invalid threshold", description: "Must be at least $1", variant: "destructive" });
                      return;
                    }
                    saveAutoReload({
                      enabled: true,
                      threshold_usd: threshold,
                      reload_amount_usd: 25,
                    });
                  }}
                >
                  {autoReloadLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : "Save"}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setEditingAutoReload(false);
                    setEditThreshold(String(autoReload.threshold_usd));
                  }}
                >
                  Cancel
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Usage History */}
      {credits && credits.usage.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6">
            <button
              onClick={() => setShowUsageHistory(!showUsageHistory)}
              className="flex items-center gap-1 text-sm font-medium text-muted-foreground uppercase tracking-wider hover:text-foreground transition-colors w-full"
            >
              <ChevronDown className={`w-3.5 h-3.5 transition-transform ${showUsageHistory ? "" : "-rotate-90"}`} />
              Recent Usage ({credits.usage.length})
            </button>
            {showUsageHistory && (
              <div className="divide-y divide-border rounded-md border border-border overflow-hidden mt-3">
                {credits.usage.slice(0, 10).map((item, i) => (
                  <div key={i} className="flex items-center justify-between px-3 py-1.5 text-xs">
                    <div className="flex items-center gap-2">
                      <span className="font-medium text-foreground">{formatModel(item.model)}</span>
                      <span className="text-muted-foreground">
                        {(() => {
                          const total = item.input_tokens + item.output_tokens;
                          return total >= 1000 ? `${(total / 1000).toFixed(1)}k` : String(total);
                        })()} tokens
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-foreground">${item.cost_usd.toFixed(4)}</span>
                      <span className="text-muted-foreground">{formatTimeAgo(item.created_at)}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default BillingPage;
