import { useState, useEffect, useCallback } from "react";
import {
  Key,
  HardDrive,
  Code,
  Info,
  Eye,
  EyeOff,
  FolderOpen,
  Trash2,
  ExternalLink,
  RefreshCw,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Cpu,
  ShoppingBag,
  Server,
  Loader2,
  Wifi,
  WifiOff,
  KeyRound,
  Plus,
  Copy,
  LogOut,
  ArrowUpCircle,
  Download,
  Terminal,
} from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { Slider } from "@/components/ui/slider";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { toast } from "@/hooks/use-toast";
import { getApiUrl, systemApi } from "@/lib/api";
import { useAuth } from "@/contexts/AuthContext";
import { useMode } from "@/contexts/ModeContext";
import ConnectivitySettings from "@/components/ConnectivitySettings";

// Empty string = same-origin (relative URLs). Works on Railway, Docker, etc.
const DEFAULT_API_URL = '';

type ConnectionStatus = "not-configured" | "valid" | "invalid" | "testing";

const SettingsPage = () => {
  // Backend URL state — empty string means "same origin" (relative URLs)
  const [apiUrl, setApiUrl] = useState(() =>
    localStorage.getItem('vectoraiz_api_url') ?? DEFAULT_API_URL
  );
  const [backendTestStatus, setBackendTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [backendErrorMessage, setBackendErrorMessage] = useState('');

  // API Keys state
  const [unstructuredKey, setUnstructuredKey] = useState("");
  const [showUnstructuredKey, setShowUnstructuredKey] = useState(false);
  const [unstructuredStatus, setUnstructuredStatus] = useState<ConnectionStatus>("not-configured");
  
  // Processing state
  const [memoryLimit, setMemoryLimit] = useState([12]);
  const [concurrentUploads, setConcurrentUploads] = useState(() =>
    localStorage.getItem('vectoraiz_concurrent_uploads') ?? 'auto'
  );
  const [recommendedConcurrent, setRecommendedConcurrent] = useState<number | null>(null);
  const [systemCores, setSystemCores] = useState<number | null>(null);
  const [systemMemGb, setSystemMemGb] = useState<number | null>(null);
  
  // Storage state
  const [dataDirectory, setDataDirectory] = useState("~/vectoraiz/data");
  
  // Developer mode state
  const [devMode, setDevMode] = useState(false);
  
  // Marketplace state
  const [marketplaceKey, setMarketplaceKey] = useState("");
  const [showMarketplaceKey, setShowMarketplaceKey] = useState(false);
  const [marketplaceConnected, setMarketplaceConnected] = useState(false);

  // Local API Keys management state
  const { logout } = useAuth();
  const { hasFeature } = useMode();
  interface ApiKeyInfo { key_id: string; label: string; scopes: string[]; created_at: string; last_used_at: string | null; revoked: boolean; }
  const [localKeys, setLocalKeys] = useState<ApiKeyInfo[]>([]);
  const [keysLoading, setKeysLoading] = useState(false);
  const [newKeyLabel, setNewKeyLabel] = useState("");
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [showCreatedKeyDialog, setShowCreatedKeyDialog] = useState(false);

  // Software update state
  interface VersionInfo {
    current: string;
    latest: string | null;
    update_available: boolean;
    latest_published_at: string | null;
    changelog_url: string;
    can_auto_update: boolean;
    error?: string;
  }
  const [versionInfo, setVersionInfo] = useState<VersionInfo | null>(null);
  const [versionLoading, setVersionLoading] = useState(false);
  const [updateStatus, setUpdateStatus] = useState<"idle" | "updating" | "done" | "error">("idle");
  const [updateMessage, setUpdateMessage] = useState("");
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const fetchLocalKeys = useCallback(async () => {
    setKeysLoading(true);
    try {
      const storedKey = localStorage.getItem('vectoraiz_api_key');
      if (!storedKey) return;
      const res = await fetch(`${getApiUrl()}/api/auth/keys`, {
        headers: { 'X-API-Key': storedKey },
      });
      if (res.ok) {
        const data = await res.json();
        setLocalKeys(data);
      }
    } catch {
      // Silently fail
    } finally {
      setKeysLoading(false);
    }
  }, []);

  const fetchVersionInfo = useCallback(async (force = false) => {
    setVersionLoading(true);
    try {
      const url = `${getApiUrl()}/api/version${force ? "?force=true" : ""}`;
      const res = await fetch(url);
      if (res.ok) {
        const data: VersionInfo = await res.json();
        setVersionInfo(data);
        setLastChecked(new Date());
      }
    } catch {
      // Silently fail
    } finally {
      setVersionLoading(false);
    }
  }, []);

  const triggerUpdate = async () => {
    setUpdateStatus("updating");
    setUpdateMessage("Pulling latest image... This may take a minute.");
    try {
      const storedKey = localStorage.getItem("vectoraiz_api_key");
      if (!storedKey) {
        setUpdateStatus("error");
        setUpdateMessage("Authentication required. Please sign in.");
        return;
      }
      const res = await fetch(`${getApiUrl()}/api/version/update`, {
        method: "POST",
        headers: { "X-API-Key": storedKey },
      });
      const data = await res.json();
      if (data.status === "updating") {
        setUpdateStatus("done");
        setUpdateMessage("Update downloaded. Restarting...");
        // Auto-refresh after 10 seconds to pick up the new version
        setTimeout(() => window.location.reload(), 10000);
      } else if (data.status === "up_to_date") {
        setUpdateStatus("idle");
        toast({ title: "Up to date", description: data.message });
      } else if (data.status === "docker_not_available") {
        setUpdateStatus("error");
        setUpdateMessage(data.message);
      } else {
        setUpdateStatus("error");
        setUpdateMessage(data.message || "Update failed.");
      }
    } catch (e) {
      setUpdateStatus("error");
      setUpdateMessage(e instanceof Error ? e.message : "Update request failed.");
    }
  };

  const createLocalKey = async () => {
    try {
      const storedKey = localStorage.getItem('vectoraiz_api_key');
      if (!storedKey) return;
      const res = await fetch(`${getApiUrl()}/api/auth/keys`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': storedKey },
        body: JSON.stringify({ label: newKeyLabel.trim() || 'My API Key' }),
      });
      if (res.ok) {
        const data = await res.json();
        setCreatedKey(data.full_key);
        setShowCreatedKeyDialog(true);
        setNewKeyLabel("");
        fetchLocalKeys();
      } else {
        const err = await res.json().catch(() => ({ detail: 'Failed to create key' }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to create API key", variant: "destructive" });
    }
  };

  const revokeLocalKey = async (keyId: string) => {
    try {
      const storedKey = localStorage.getItem('vectoraiz_api_key');
      if (!storedKey) return;
      const res = await fetch(`${getApiUrl()}/api/auth/keys/${keyId}`, {
        method: 'DELETE',
        headers: { 'X-API-Key': storedKey },
      });
      if (res.ok) {
        toast({ title: "Key revoked", description: `API key ${keyId} has been revoked.` });
        fetchLocalKeys();
      } else {
        const err = await res.json().catch(() => ({ detail: 'Failed to revoke key' }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to revoke API key", variant: "destructive" });
    }
  };

  // Resolve display/test URL: empty string → current origin
  const resolvedUrl = apiUrl || window.location.origin;

  // Test backend connection on mount
  useEffect(() => {
    const testInitialConnection = async () => {
      try {
        const url = apiUrl || window.location.origin;
        const response = await fetch(`${url}/api/health`);
        if (response.ok) {
          setBackendTestStatus('success');
        }
      } catch {
        // Silently fail on initial check
      }
    };
    testInitialConnection();
  }, []);

  // Fetch local API keys on mount
  useEffect(() => {
    fetchLocalKeys();
  }, [fetchLocalKeys]);

  // Fetch version info on mount
  useEffect(() => {
    fetchVersionInfo();
  }, [fetchVersionInfo]);

  // Fetch system info for recommended concurrent uploads
  useEffect(() => {
    systemApi.info().then((info) => {
      const rec = info.system.recommended_concurrent_uploads;
      setRecommendedConcurrent(rec);
      setSystemCores(info.system.cpu_cores);
      setSystemMemGb(info.system.memory_gb);
      localStorage.setItem('vectoraiz_recommended_concurrent', String(rec));
    }).catch(() => {
      // Fallback: leave recommendation as null
    });
  }, []);

  // Validate URL format
  const isValidUrl = (url: string): boolean => {
    try {
      const parsed = new URL(url);
      return parsed.protocol === 'http:' || parsed.protocol === 'https:';
    } catch {
      return false;
    }
  };

  const testBackendConnection = async () => {
    // When apiUrl is non-empty, validate it's a proper URL
    if (apiUrl && !isValidUrl(apiUrl)) {
      setBackendTestStatus('error');
      setBackendErrorMessage('Invalid URL format. Use http:// or https://');
      return;
    }

    setBackendTestStatus('testing');
    setBackendErrorMessage('');

    const testUrl = apiUrl || window.location.origin;

    try {
      const response = await fetch(`${testUrl}/api/health`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      });

      if (response.ok) {
        setBackendTestStatus('success');
        // Auto-save on successful connection
        if (apiUrl) {
          localStorage.setItem('vectoraiz_api_url', apiUrl);
        } else {
          localStorage.removeItem('vectoraiz_api_url');
        }
        toast({
          title: "Backend connected",
          description: "Successfully connected to vectorAIz backend.",
        });
      } else {
        setBackendTestStatus('error');
        setBackendErrorMessage(`Server returned ${response.status}`);
      }
    } catch (e) {
      setBackendTestStatus('error');
      setBackendErrorMessage(e instanceof Error ? e.message : 'Connection failed');
    }
  };

  const handleResetBackendUrl = () => {
    setApiUrl(DEFAULT_API_URL);
    localStorage.removeItem('vectoraiz_api_url');
    setBackendTestStatus('idle');
    toast({
      title: "Reset to default",
      description: "Backend URL reset to same-origin (auto-detect)",
    });
  };

  const handleSaveBackendUrl = () => {
    if (apiUrl) {
      localStorage.setItem('vectoraiz_api_url', apiUrl);
    } else {
      localStorage.removeItem('vectoraiz_api_url');
    }
    toast({
      title: "Backend URL saved",
      description: apiUrl ? "Your backend URL has been saved." : "Using same-origin (auto-detect).",
    });
  };

  const testUnstructuredConnection = () => {
    setUnstructuredStatus("testing");
    setTimeout(() => {
      setUnstructuredStatus(unstructuredKey.length > 10 ? "valid" : "invalid");
    }, 1500);
  };

  const getStatusBadge = (status: ConnectionStatus) => {
    switch (status) {
      case "not-configured":
        return (
          <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className="w-2 h-2 rounded-full bg-muted-foreground" />
            Not configured
          </span>
        );
      case "valid":
        return (
          <span className="flex items-center gap-1.5 text-xs text-[hsl(var(--haven-success))]">
            <CheckCircle className="w-3.5 h-3.5" />
            Valid
          </span>
        );
      case "invalid":
        return (
          <span className="flex items-center gap-1.5 text-xs text-destructive">
            <XCircle className="w-3.5 h-3.5" />
            Invalid
          </span>
        );
      case "testing":
        return (
          <span className="flex items-center gap-1.5 text-xs text-primary">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" />
            Testing...
          </span>
        );
    }
  };

  const handleSaveSettings = () => {
    toast({
      title: "Settings saved successfully",
      description: "Your configuration has been updated.",
    });
  };

  const handleClearCache = () => {
    toast({
      title: "Cache cleared",
      description: "All cached data has been removed.",
    });
  };

  return (
    <div className="space-y-6 max-w-3xl pb-20">
      <div>
        <p className="text-muted-foreground">
          Configure vectorAIz to match your workflow
        </p>
      </div>

      {/* Section 0: Backend Connection */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Server className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Backend Connection</CardTitle>
              <CardDescription>Configure the vectorAIz backend API URL</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="backend-url" className="text-foreground">Backend URL</Label>
            <div className="flex gap-2">
              <Input
                id="backend-url"
                type="text"
                placeholder={`Same origin (${window.location.origin})`}
                value={apiUrl}
                onChange={(e) => {
                  setApiUrl(e.target.value);
                  setBackendTestStatus('idle');
                }}
                className="flex-1 bg-background border-border text-foreground font-mono"
              />
              <Button
                variant="outline"
                onClick={testBackendConnection}
                disabled={backendTestStatus === 'testing'}
              >
                {backendTestStatus === 'testing' ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  'Test'
                )}
              </Button>
            </div>
          </div>

          {/* Connection status */}
          {backendTestStatus === 'success' && (
            <div className="flex items-center gap-2 text-sm text-[hsl(var(--haven-success))]">
              <Wifi className="w-4 h-4" />
              Connected successfully
            </div>
          )}
          {backendTestStatus === 'error' && (
            <div className="flex items-center gap-2 text-sm text-destructive">
              <WifiOff className="w-4 h-4" />
              {backendErrorMessage || 'Connection failed'}
            </div>
          )}

          <div className="flex gap-2 pt-2">
            <Button variant="outline" size="sm" onClick={handleSaveBackendUrl}>
              Save
            </Button>
            <Button variant="ghost" size="sm" onClick={handleResetBackendUrl}>
              Reset to Default
            </Button>
          </div>

          <p className="text-xs text-muted-foreground">
            Default: same origin (auto-detect). Leave empty when the frontend is served from the backend.
            Only set a custom URL if the backend is on a different host.
          </p>
        </CardContent>
      </Card>

      {/* Section: Local API Keys Management */}
      <Card className="bg-card border-border" id="api-keys">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <KeyRound className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">API Keys</CardTitle>
              <CardDescription>Manage your vectorAIz API keys for programmatic access</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Create new key */}
          <div className="flex gap-2">
            <Input
              placeholder="Key label (e.g. CI/CD)"
              value={newKeyLabel}
              onChange={(e) => setNewKeyLabel(e.target.value)}
              className="flex-1 bg-background border-border"
            />
            <Button onClick={createLocalKey} className="gap-2">
              <Plus className="w-4 h-4" />
              Create Key
            </Button>
          </div>

          {/* Key list */}
          {keysLoading ? (
            <div className="flex items-center justify-center py-6">
              <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
            </div>
          ) : localKeys.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4 text-center">No API keys found.</p>
          ) : (
            <div className="space-y-2">
              {localKeys.filter(k => !k.revoked).map((key) => {
                const isSystemKey = key.label === "Admin (setup)" || key.label?.startsWith("Login (");
                return (
                <div key={key.key_id} className="flex items-center justify-between p-3 bg-secondary/50 rounded-lg">
                  <div className="space-y-0.5 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-foreground">{key.label}</span>
                      {isSystemKey && (
                        <span className="text-[10px] font-medium px-1.5 py-0.5 rounded bg-primary/10 text-primary border border-primary/20">System</span>
                      )}
                      <span className="text-xs font-mono text-muted-foreground">vz_{key.key_id}_****</span>
                    </div>
                    <div className="text-xs text-muted-foreground">
                      Created {new Date(key.created_at).toLocaleDateString()}
                      {key.last_used_at && ` \u00B7 Last used ${new Date(key.last_used_at).toLocaleDateString()}`}
                    </div>
                  </div>
                  {isSystemKey ? (
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button variant="ghost" size="sm" className="text-muted-foreground" disabled>
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>System keys cannot be deleted</TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  ) : (
                  <AlertDialog>
                    <AlertDialogTrigger asChild>
                      <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent>
                      <AlertDialogHeader>
                        <AlertDialogTitle>Revoke API Key</AlertDialogTitle>
                        <AlertDialogDescription>
                          This will permanently revoke the key <span className="font-mono">vz_{key.key_id}_****</span>.
                          Any applications using this key will lose access immediately.
                        </AlertDialogDescription>
                      </AlertDialogHeader>
                      <AlertDialogFooter>
                        <AlertDialogCancel>Cancel</AlertDialogCancel>
                        <AlertDialogAction onClick={() => revokeLocalKey(key.key_id)} className="bg-destructive hover:bg-destructive/90">
                          Revoke Key
                        </AlertDialogAction>
                      </AlertDialogFooter>
                    </AlertDialogContent>
                  </AlertDialog>
                  )}
                </div>
                );
              })}
            </div>
          )}

          {/* Logout button */}
          <div className="pt-2 border-t border-border">
            <Button variant="outline" className="gap-2" onClick={logout}>
              <LogOut className="w-4 h-4" />
              Sign Out
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Created key dialog — show the full key ONCE */}
      <Dialog open={showCreatedKeyDialog} onOpenChange={setShowCreatedKeyDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>API Key Created</DialogTitle>
            <DialogDescription>
              Copy this key now. You won't be able to see it again.
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center gap-2">
            <code className="flex-1 p-3 bg-secondary rounded-lg text-sm font-mono break-all text-foreground">
              {createdKey}
            </code>
            <Button
              variant="outline"
              size="icon"
              onClick={() => {
                if (createdKey) {
                  navigator.clipboard.writeText(createdKey);
                  toast({ title: "Copied to clipboard" });
                }
              }}
            >
              <Copy className="w-4 h-4" />
            </Button>
          </div>
          <DialogFooter>
            <Button onClick={() => setShowCreatedKeyDialog(false)}>Done</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Section 1: External Service API Keys */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Key className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Service Configuration</CardTitle>
              <CardDescription>Connect optional services to enhance your vectorAIz experience</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Unstructured API Key */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="unstructured" className="text-foreground font-medium">
                  Document Parser (Unstructured)
                </Label>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info className="w-4 h-4 text-muted-foreground cursor-help" />
                    </TooltipTrigger>
                    <TooltipContent side="top" className="max-w-xs">
                      <p>Unstructured.io provides enhanced parsing for complex PDFs, scanned documents, and images. Without this key, vectorAIz uses its built-in parser which works well for most documents.</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </div>
              {getStatusBadge(unstructuredStatus)}
            </div>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Input
                  id="unstructured"
                  type={showUnstructuredKey ? "text" : "password"}
                  placeholder="Enter your Unstructured API key"
                  value={unstructuredKey}
                  onChange={(e) => {
                    setUnstructuredKey(e.target.value);
                    setUnstructuredStatus(e.target.value ? "not-configured" : "not-configured");
                  }}
                  className="bg-background border-border text-foreground pr-10"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7"
                  onClick={() => setShowUnstructuredKey(!showUnstructuredKey)}
                >
                  {showUnstructuredKey ? (
                    <EyeOff className="w-4 h-4 text-muted-foreground" />
                  ) : (
                    <Eye className="w-4 h-4 text-muted-foreground" />
                  )}
                </Button>
              </div>
              <Button
                variant="outline"
                onClick={testUnstructuredConnection}
                disabled={!unstructuredKey || unstructuredStatus === "testing"}
              >
                Test Connection
              </Button>
            </div>
            <a
              href="https://unstructured.io"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
            >
              Get a free API key
              <ExternalLink className="w-3 h-3" />
            </a>
          </div>

          <div className="pt-2 border-t border-border">
            <p className="text-xs text-muted-foreground">
              For additional format support, see{" "}
              <a href="/data-types" className="text-primary hover:underline">Data Types</a>.
              External LLMs connect to vectorAIz via MCP or REST.
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Section 2: Processing Settings */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Cpu className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Processing</CardTitle>
              <CardDescription>Configure data processing settings</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <Label className="text-foreground">Memory Limit</Label>
              <span className="text-sm font-mono text-primary">{memoryLimit[0]} GB</span>
            </div>
            <Slider
              value={memoryLimit}
              onValueChange={setMemoryLimit}
              min={4}
              max={32}
              step={2}
              className="w-full"
            />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>4 GB</span>
              <span>32 GB</span>
            </div>
            <p className="text-sm text-muted-foreground">
              Maximum memory for data processing. Higher values allow larger files.
            </p>
          </div>

          <div className="space-y-2 pt-2 border-t border-border">
            <Label className="text-foreground">Concurrent Uploads</Label>
            <Select value={concurrentUploads} onValueChange={(v) => {
              setConcurrentUploads(v);
              if (v === 'auto') {
                localStorage.removeItem('vectoraiz_concurrent_uploads');
              } else {
                localStorage.setItem('vectoraiz_concurrent_uploads', v);
              }
            }}>
              <SelectTrigger className="bg-background border-border w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">Auto{recommendedConcurrent ? ` (${recommendedConcurrent})` : ''}</SelectItem>
                <SelectItem value="1">1</SelectItem>
                <SelectItem value="2">2</SelectItem>
                <SelectItem value="3">3</SelectItem>
                <SelectItem value="4">4</SelectItem>
                <SelectItem value="5">5</SelectItem>
                <SelectItem value="6">6</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-sm text-muted-foreground">
              Number of files uploaded simultaneously.
              {systemCores !== null && systemMemGb !== null && (
                <> Detected: {systemCores} cores, {systemMemGb} GB RAM.</>
              )}
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Section 3: Storage */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <HardDrive className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Storage</CardTitle>
              <CardDescription>Configure data storage location</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="data-dir" className="text-foreground">Data Directory</Label>
            <div className="flex gap-2">
              <Input
                id="data-dir"
                type="text"
                value={dataDirectory}
                onChange={(e) => setDataDirectory(e.target.value)}
                className="bg-background border-border text-foreground font-mono"
              />
              <Button variant="outline" className="gap-2">
                <FolderOpen className="w-4 h-4" />
                Browse
              </Button>
            </div>
          </div>

          <div className="space-y-2 pt-2 border-t border-border">
            <div className="flex items-center justify-between">
              <Label className="text-foreground">Current Usage</Label>
              <span className="text-sm text-muted-foreground">2.4 GB of 100 GB used</span>
            </div>
            <Progress value={2.4} max={100} className="h-2" />
          </div>

          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="outline" className="gap-2 text-destructive hover:text-destructive">
                <Trash2 className="w-4 h-4" />
                Clear Cache
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Clear Cache</AlertDialogTitle>
                <AlertDialogDescription>
                  This will remove all cached data including temporary files and processed data. 
                  Your original datasets will not be affected. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={handleClearCache} className="bg-destructive hover:bg-destructive/90">
                  Clear Cache
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </CardContent>
      </Card>

      {/* Section: External Connectivity */}
      <ConnectivitySettings />

      {/* Section 4: Developer Mode */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <Code className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Developer Mode</CardTitle>
              <CardDescription>Advanced features for developers</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5">
              <Label className="text-foreground">Expose Vector Database</Label>
              <p className="text-sm text-muted-foreground">
                Make Qdrant accessible for external connections
              </p>
            </div>
            <Switch checked={devMode} onCheckedChange={setDevMode} />
          </div>

          {devMode && (
            <>
              <div className="flex items-start gap-2 p-3 bg-[hsl(var(--haven-warning))]/10 border border-[hsl(var(--haven-warning))]/30 rounded-lg">
                <AlertTriangle className="w-4 h-4 text-[hsl(var(--haven-warning))] mt-0.5 flex-shrink-0" />
                <p className="text-sm text-[hsl(var(--haven-warning))]">
                  Qdrant will be accessible on localhost:6333. Only enable for development.
                </p>
              </div>

              <div className="p-4 bg-secondary/50 rounded-lg space-y-3">
                <h4 className="text-sm font-medium text-foreground">Connection Info</h4>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <span className="text-muted-foreground">Host:</span>
                  <span className="font-mono text-foreground">localhost</span>
                  <span className="text-muted-foreground">Port:</span>
                  <span className="font-mono text-foreground">6333</span>
                </div>
                <div className="pt-2 border-t border-border">
                  <p className="text-xs text-muted-foreground mb-2">Example connection:</p>
                  <pre className="text-xs font-mono bg-background p-2 rounded border border-border overflow-x-auto">
{`from qdrant_client import QdrantClient
client = QdrantClient(host="localhost", port=6333)`}
                  </pre>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Section 5: Marketplace (only in connected mode) */}
      {hasFeature("marketplace") && (
        <Card className="bg-card border-border">
          <CardHeader>
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                <ShoppingBag className="w-5 h-5 text-primary" />
              </div>
              <div>
                <CardTitle className="text-foreground">Marketplace Connection</CardTitle>
                <CardDescription>Connect to the marketplace to buy and sell datasets</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="marketplace-key" className="text-foreground">API Key</Label>
              <div className="relative">
                <Input
                  id="marketplace-key"
                  type={showMarketplaceKey ? "text" : "password"}
                  placeholder="Enter your marketplace API key"
                  value={marketplaceKey}
                  onChange={(e) => setMarketplaceKey(e.target.value)}
                  className="bg-background border-border text-foreground pr-10"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7"
                  onClick={() => setShowMarketplaceKey(!showMarketplaceKey)}
                >
                  {showMarketplaceKey ? (
                    <EyeOff className="w-4 h-4 text-muted-foreground" />
                  ) : (
                    <Eye className="w-4 h-4 text-muted-foreground" />
                  )}
                </Button>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Status:</span>
              {marketplaceConnected ? (
                <span className="flex items-center gap-1.5 text-sm text-[hsl(var(--haven-success))]">
                  <CheckCircle className="w-4 h-4" />
                  Connected as seller@example.com
                </span>
              ) : (
                <span className="text-sm text-muted-foreground">Not connected</span>
              )}
            </div>

            <a
              href="#"
              className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
              onClick={(e) => {
                e.preventDefault();
                toast({ title: "Redirecting to marketplace signup..." });
              }}
            >
              Create Account
              <ExternalLink className="w-3 h-3" />
            </a>
          </CardContent>
        </Card>
      )}

      {/* Section 6: Software Updates & About */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
              <ArrowUpCircle className="w-5 h-5 text-primary" />
            </div>
            <div>
              <CardTitle className="text-foreground">Software Updates</CardTitle>
              <CardDescription>Application information and updates</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Version info */}
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Current Version</span>
              <span className="text-foreground font-mono">{versionInfo?.current ?? "..."}</span>
            </div>
            <div className="flex justify-between text-sm items-center">
              <span className="text-muted-foreground">Latest Version</span>
              <div className="flex items-center gap-2">
                <span className="text-foreground font-mono">{versionInfo?.latest ?? "..."}</span>
                {versionInfo?.update_available && (
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs font-medium rounded-full bg-[hsl(var(--haven-warning))]/15 text-[hsl(var(--haven-warning))]">
                    Update available
                  </span>
                )}
                {versionInfo && !versionInfo.update_available && !versionInfo.error && (
                  <span className="inline-flex items-center gap-1 text-xs text-[hsl(var(--haven-success))]">
                    <CheckCircle className="w-3.5 h-3.5" />
                    Up to date
                  </span>
                )}
              </div>
            </div>
            {lastChecked && (
              <div className="flex justify-between text-sm">
                <span className="text-muted-foreground">Last Checked</span>
                <span className="text-muted-foreground text-xs">
                  {lastChecked.toLocaleTimeString()}
                </span>
              </div>
            )}
          </div>

          {/* Error state */}
          {versionInfo?.error && (
            <div className="flex items-start gap-2 p-3 bg-destructive/10 border border-destructive/30 rounded-lg">
              <XCircle className="w-4 h-4 text-destructive mt-0.5 flex-shrink-0" />
              <p className="text-sm text-destructive">
                Could not check for updates: {versionInfo.error}
              </p>
            </div>
          )}

          {/* Update status messages */}
          {updateStatus === "updating" && (
            <div className="flex items-center gap-2 p-3 bg-primary/10 border border-primary/30 rounded-lg">
              <Loader2 className="w-4 h-4 text-primary animate-spin flex-shrink-0" />
              <p className="text-sm text-primary">{updateMessage}</p>
            </div>
          )}
          {updateStatus === "done" && (
            <div className="flex items-center gap-2 p-3 bg-[hsl(var(--haven-success))]/10 border border-[hsl(var(--haven-success))]/30 rounded-lg">
              <CheckCircle className="w-4 h-4 text-[hsl(var(--haven-success))] flex-shrink-0" />
              <p className="text-sm text-[hsl(var(--haven-success))]">{updateMessage}</p>
            </div>
          )}
          {updateStatus === "error" && (
            <div className="flex items-start gap-2 p-3 bg-destructive/10 border border-destructive/30 rounded-lg">
              <XCircle className="w-4 h-4 text-destructive mt-0.5 flex-shrink-0" />
              <p className="text-sm text-destructive whitespace-pre-wrap">{updateMessage}</p>
            </div>
          )}

          {/* Manual update instructions (when Docker socket not available) */}
          {versionInfo && !versionInfo.can_auto_update && versionInfo.update_available && (
            <div className="space-y-2 p-3 bg-secondary/50 rounded-lg">
              <div className="flex items-center gap-2">
                <Terminal className="w-4 h-4 text-muted-foreground" />
                <span className="text-sm font-medium text-foreground">Manual update</span>
              </div>
              <pre className="text-xs font-mono bg-background p-2 rounded border border-border overflow-x-auto">
{`cd your-vectoraiz-directory
docker compose -f docker-compose.customer.yml pull vectoraiz
docker compose -f docker-compose.customer.yml up -d vectoraiz`}
              </pre>
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-2">
            <Button
              variant="outline"
              className="gap-2"
              disabled={versionLoading}
              onClick={() => fetchVersionInfo(true)}
            >
              {versionLoading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <RefreshCw className="w-4 h-4" />
              )}
              Check Now
            </Button>
            {versionInfo?.update_available && versionInfo?.can_auto_update && (
              <Button
                className="gap-2"
                disabled={updateStatus === "updating" || updateStatus === "done"}
                onClick={triggerUpdate}
              >
                {updateStatus === "updating" ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Download className="w-4 h-4" />
                )}
                Update to {versionInfo.latest}
              </Button>
            )}
          </div>

          {/* About footer */}
          <div className="pt-3 border-t border-border space-y-3">
            <p className="text-sm text-muted-foreground">
              vectorAIz{versionInfo?.current ? ` v${versionInfo.current}` : ""}
            </p>
            <div className="flex flex-wrap gap-4">
              <a
                href="https://github.com/aidotmarket/vectoraiz"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
              >
                Documentation
                <ExternalLink className="w-3 h-3" />
              </a>
              <a
                href="https://github.com/aidotmarket/vectoraiz"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
              >
                GitHub
                <ExternalLink className="w-3 h-3" />
              </a>
              <a
                href="https://github.com/aidotmarket/vectoraiz/issues"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
              >
                Report Issue
                <ExternalLink className="w-3 h-3" />
              </a>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Sticky Save Button */}
      <div className="fixed bottom-0 left-0 right-0 p-4 bg-background/95 backdrop-blur border-t border-border">
        <div className="max-w-3xl mx-auto flex justify-end">
          <Button onClick={handleSaveSettings} className="gap-2">
            Save Settings
          </Button>
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;