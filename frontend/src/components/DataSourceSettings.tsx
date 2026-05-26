import { useCallback, useEffect, useState } from "react";
import {
  CheckCircle,
  Copy,
  Database,
  Loader2,
  Plus,
  RefreshCw,
  Trash2,
  XCircle,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { toast } from "@/hooks/use-toast";
import { getApiUrl } from "@/lib/api";

interface S3Connection {
  id: string;
  name: string;
  bucket: string;
  region: string;
  prefix?: string | null;
  role_arn?: string | null;
  external_id?: string | null;
  status: "onboarding" | "configured" | "verified" | "error" | string;
  error_message?: string | null;
  last_scanned_at?: string | null;
  trust_policy?: Record<string, unknown> | null;
  permission_policy?: Record<string, unknown> | null;
}

interface VerifyResult {
  status: string;
  error_message?: string | null;
  verified_at?: string | null;
}

const REGIONS = [
  "us-east-1",
  "us-east-2",
  "us-west-1",
  "us-west-2",
  "eu-west-1",
  "eu-central-1",
  "ap-southeast-1",
  "ap-northeast-1",
];

function apiHeaders(): Record<string, string> {
  const accessToken = localStorage.getItem("aim_data_access_token");
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (accessToken) headers["Authorization"] = `Bearer ${accessToken}`;
  return headers;
}

function formatDate(value?: string | null) {
  if (!value) return "Not verified";
  return new Date(value).toLocaleString();
}

function statusBadge(status: S3Connection["status"]) {
  if (status === "verified") {
    return <Badge className="bg-[hsl(var(--haven-success))]/15 text-[hsl(var(--haven-success))] hover:bg-[hsl(var(--haven-success))]/20">Verified</Badge>;
  }
  if (status === "configured") {
    return <Badge className="bg-blue-500/15 text-blue-600 hover:bg-blue-500/20">Configured</Badge>;
  }
  if (status === "error") {
    return <Badge variant="destructive">Error</Badge>;
  }
  return <Badge variant="secondary">Onboarding</Badge>;
}

function PolicyBlock({ label, value }: { label: string; value: string }) {
  const copy = async () => {
    await navigator.clipboard.writeText(value);
    toast({ title: "Copied", description: `${label} copied to clipboard.` });
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-3">
        <Label className="text-foreground">{label}</Label>
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={copy}>
          <Copy className="w-3.5 h-3.5" />
          Copy
        </Button>
      </div>
      <pre className="max-h-44 overflow-auto rounded-md border border-border bg-secondary/50 p-3 text-xs text-foreground">
        {value}
      </pre>
    </div>
  );
}

export default function DataSourceSettings() {
  const [connections, setConnections] = useState<S3Connection[]>([]);
  const [loading, setLoading] = useState(true);
  const [showDialog, setShowDialog] = useState(false);
  const [step, setStep] = useState(1);
  const [draft, setDraft] = useState({ name: "", bucket: "", region: "us-east-1", prefix: "" });
  const [current, setCurrent] = useState<S3Connection | null>(null);
  const [awsAccountId, setAwsAccountId] = useState("");
  const [roleArn, setRoleArn] = useState("");
  const [roleError, setRoleError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [verifyingId, setVerifyingId] = useState<string | null>(null);
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null);

  const fetchConnections = useCallback(async () => {
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/`, { headers: apiHeaders() });
      if (response.ok) {
        setConnections(await response.json());
      }
    } catch {
      toast({ title: "Error", description: "Failed to load data sources", variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchConfig = useCallback(async () => {
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/config`, { headers: apiHeaders() });
      if (response.ok) {
        const data = await response.json();
        setAwsAccountId(data.aws_account_id);
      }
    } catch {
      // The policy response also includes the account ARN; keep the dialog usable.
    }
  }, []);

  useEffect(() => {
    fetchConnections();
    fetchConfig();
  }, [fetchConnections, fetchConfig]);

  const resetWizard = () => {
    setStep(1);
    setDraft({ name: "", bucket: "", region: "us-east-1", prefix: "" });
    setCurrent(null);
    setRoleArn("");
    setRoleError("");
    setVerifyResult(null);
  };

  const openWizard = () => {
    resetWizard();
    setShowDialog(true);
  };

  const createConnection = async () => {
    if (!draft.name.trim() || !draft.bucket.trim()) return;
    setSubmitting(true);
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/`, {
        method: "POST",
        headers: apiHeaders(),
        body: JSON.stringify({
          name: draft.name.trim(),
          bucket: draft.bucket.trim(),
          region: draft.region,
          prefix: draft.prefix.trim() || null,
        }),
      });
      if (response.ok) {
        setCurrent(await response.json());
        setStep(2);
      } else {
        const err = await response.json().catch(() => ({ detail: "Failed to create connection" }));
        toast({ title: "Error", description: err.detail, variant: "destructive" });
      }
    } catch {
      toast({ title: "Error", description: "Failed to create connection", variant: "destructive" });
    } finally {
      setSubmitting(false);
    }
  };

  const saveRoleArn = async () => {
    if (!current) return;
    setSubmitting(true);
    setRoleError("");
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/${current.id}/role-arn`, {
        method: "PUT",
        headers: apiHeaders(),
        body: JSON.stringify({ role_arn: roleArn.trim() }),
      });
      if (response.ok) {
        setCurrent(await response.json());
        setStep(4);
      } else {
        const err = await response.json().catch(() => ({ detail: "Invalid role ARN" }));
        setRoleError(err.detail || "Invalid role ARN");
      }
    } catch {
      setRoleError("Failed to save role ARN");
    } finally {
      setSubmitting(false);
    }
  };

  const verifyConnection = async (connectionId: string, fromWizard = false) => {
    setVerifyingId(connectionId);
    if (fromWizard) setVerifyResult(null);
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/${connectionId}/verify`, {
        method: "POST",
        headers: apiHeaders(),
      });
      const data = await response.json();
      if (response.ok) {
        if (fromWizard) setVerifyResult(data);
        if (data.status === "verified") {
          toast({ title: "Connection verified", description: "AWS access confirmed." });
          fetchConnections();
          if (fromWizard) {
            setTimeout(() => {
              setShowDialog(false);
              resetWizard();
              fetchConnections();
            }, 2000);
          }
        } else if (!fromWizard) {
          toast({ title: "Verification failed", description: data.error_message, variant: "destructive" });
          fetchConnections();
        }
      }
    } catch {
      const failed = { status: "error", error_message: "Verification request failed" };
      if (fromWizard) setVerifyResult(failed);
      toast({ title: "Error", description: failed.error_message, variant: "destructive" });
    } finally {
      setVerifyingId(null);
    }
  };

  const deleteConnection = async (connectionId: string) => {
    try {
      const response = await fetch(`${getApiUrl()}/api/s3-connections/${connectionId}`, {
        method: "DELETE",
        headers: apiHeaders(),
      });
      if (response.ok) {
        toast({ title: "Data source deleted", description: "The S3 connection was removed." });
        fetchConnections();
      }
    } catch {
      toast({ title: "Error", description: "Failed to delete data source", variant: "destructive" });
    }
  };

  const accountBlock = awsAccountId || "000000000000";
  const trustPolicy = current?.trust_policy ? JSON.stringify(current.trust_policy, null, 2) : "";
  const permissionPolicy = current?.permission_policy ? JSON.stringify(current.permission_policy, null, 2) : "";

  return (
    <Card className="bg-card border-border">
      <CardHeader>
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
            <Database className="w-5 h-5 text-primary" />
          </div>
          <div className="flex-1">
            <CardTitle className="text-foreground">Data Sources</CardTitle>
            <CardDescription>Connect seller-owned buckets using AWS STS AssumeRole</CardDescription>
          </div>
          <Button size="sm" className="gap-1.5" onClick={openWizard}>
            <Plus className="w-3.5 h-3.5" />
            Add S3 Connection
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {loading ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
          </div>
        ) : connections.length === 0 ? (
          <p className="py-6 text-center text-sm text-muted-foreground">No data sources connected yet.</p>
        ) : (
          <div className="space-y-2">
            {connections.map((connection) => (
              <div key={connection.id} className="flex flex-col gap-3 rounded-lg bg-secondary/50 p-3 md:flex-row md:items-center md:justify-between">
                <div className="min-w-0 space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-medium text-foreground">{connection.name}</span>
                    {statusBadge(connection.status)}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    <span className="font-mono">{connection.bucket}</span>
                    {connection.prefix ? <span className="font-mono">/{connection.prefix}</span> : null}
                    <span> · {connection.region}</span>
                    <span> · Last verified {formatDate(connection.last_scanned_at)}</span>
                  </div>
                  {connection.error_message ? (
                    <p className="line-clamp-2 text-xs text-destructive">{connection.error_message}</p>
                  ) : null}
                </div>
                <div className="flex items-center gap-1.5">
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-1.5"
                    disabled={verifyingId === connection.id || !connection.role_arn}
                    onClick={() => verifyConnection(connection.id)}
                  >
                    {verifyingId === connection.id ? (
                      <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    ) : (
                      <RefreshCw className="w-3.5 h-3.5" />
                    )}
                    Re-verify
                  </Button>
                  <AlertDialog>
                    <AlertDialogTrigger asChild>
                      <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent>
                      <AlertDialogHeader>
                        <AlertDialogTitle>Delete Data Source</AlertDialogTitle>
                        <AlertDialogDescription>
                          This removes the local S3 connection record for {connection.name}. The AWS IAM role is not changed.
                        </AlertDialogDescription>
                      </AlertDialogHeader>
                      <AlertDialogFooter>
                        <AlertDialogCancel>Cancel</AlertDialogCancel>
                        <AlertDialogAction
                          className="bg-destructive hover:bg-destructive/90"
                          onClick={() => deleteConnection(connection.id)}
                        >
                          Delete
                        </AlertDialogAction>
                      </AlertDialogFooter>
                    </AlertDialogContent>
                  </AlertDialog>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>

      <Dialog open={showDialog} onOpenChange={(open) => {
        setShowDialog(open);
        if (!open) resetWizard();
      }}>
        <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>Add S3 Connection</DialogTitle>
            <DialogDescription>Step {step} of 4</DialogDescription>
          </DialogHeader>

          {step === 1 && (
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="s3-name">Name</Label>
                  <Input id="s3-name" value={draft.name} onChange={(event) => setDraft({ ...draft, name: event.target.value })} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="s3-bucket">Bucket</Label>
                  <Input id="s3-bucket" value={draft.bucket} onChange={(event) => setDraft({ ...draft, bucket: event.target.value })} />
                </div>
              </div>
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label>Region</Label>
                  <Select value={draft.region} onValueChange={(region) => setDraft({ ...draft, region })}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {REGIONS.map((region) => (
                        <SelectItem key={region} value={region}>{region}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="s3-prefix">Prefix</Label>
                  <Input
                    id="s3-prefix"
                    placeholder="leave empty to scan whole bucket"
                    value={draft.prefix}
                    onChange={(event) => setDraft({ ...draft, prefix: event.target.value })}
                  />
                </div>
              </div>
            </div>
          )}

          {step === 2 && current && (
            <div className="space-y-4">
              <ol className="list-decimal space-y-2 pl-5 text-sm text-foreground">
                <li>In AWS Console &gt; IAM &gt; Roles &gt; Create role &gt; AWS account &gt; Another AWS account. Paste this account ID and the external ID below.</li>
                <li>Copy this trust policy and paste it as the role&apos;s trust policy:</li>
                <li>Attach this as an inline permission policy on the role:</li>
              </ol>
              <PolicyBlock label="Account ID" value={accountBlock} />
              <PolicyBlock label="External ID" value={current.external_id || ""} />
              <PolicyBlock label="Trust policy" value={trustPolicy} />
              <PolicyBlock label="Permission policy" value={permissionPolicy} />
            </div>
          )}

          {step === 3 && (
            <div className="space-y-3">
              <div className="space-y-2">
                <Label htmlFor="role-arn">Role ARN</Label>
                <Input
                  id="role-arn"
                  placeholder="arn:aws:iam::123456789012:role/your-role-name"
                  value={roleArn}
                  onChange={(event) => setRoleArn(event.target.value)}
                />
              </div>
              {roleError ? <p className="text-sm text-destructive">{roleError}</p> : null}
            </div>
          )}

          {step === 4 && current && (
            <div className="space-y-4">
              <Button
                className="gap-2"
                disabled={verifyingId === current.id}
                onClick={() => verifyConnection(current.id, true)}
              >
                {verifyingId === current.id ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <RefreshCw className="w-4 h-4" />
                )}
                Verify Connection
              </Button>
              {verifyResult?.status === "verified" ? (
                <div className="flex items-start gap-2 rounded-lg border border-[hsl(var(--haven-success))]/30 bg-[hsl(var(--haven-success))]/10 p-3 text-sm text-[hsl(var(--haven-success))]">
                  <CheckCircle className="mt-0.5 h-4 w-4 flex-shrink-0" />
                  <span>Connection verified! AWS access confirmed and one object listed from your bucket.</span>
                </div>
              ) : null}
              {verifyResult?.status === "error" ? (
                <div className="space-y-3">
                  <div className="flex items-start gap-2 rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                    <XCircle className="mt-0.5 h-4 w-4 flex-shrink-0" />
                    <span>{verifyResult.error_message || "Verification failed"}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button variant="outline" size="sm" onClick={() => verifyConnection(current.id, true)}>
                      Retry
                    </Button>
                    <Button variant="link" size="sm" onClick={() => setStep(2)}>
                      Back to Step 2
                    </Button>
                  </div>
                </div>
              ) : null}
            </div>
          )}

          <DialogFooter>
            {step === 1 ? (
              <Button disabled={!draft.name.trim() || !draft.bucket.trim() || submitting} onClick={createConnection}>
                {submitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Next
              </Button>
            ) : null}
            {step === 2 ? <Button onClick={() => setStep(3)}>Next</Button> : null}
            {step === 3 ? (
              <Button disabled={!roleArn.trim() || submitting} onClick={saveRoleArn}>
                {submitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Save
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
