import { useState, useEffect } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import {
  ArrowLeft,
  FileText,
  FileImage,
  FileAudio,
  File,
  Download,
  Trash2,
  Save,
  Loader2,
  CheckCircle2,
  XCircle,
  Zap,
  ShoppingCart,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { rawFilesApi, type RawFile } from "@/lib/api";
import { toast } from "@/hooks/use-toast";
import { useMode } from "@/contexts/ModeContext";

function getFileIcon(mimeType: string | null) {
  if (!mimeType) return File;
  if (mimeType.startsWith("image/")) return FileImage;
  if (mimeType.startsWith("audio/")) return FileAudio;
  if (mimeType === "application/pdf") return FileText;
  return File;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
}

function getListingStatusBadge(status: string | null) {
  if (!status) return <Badge variant="outline">No Listing</Badge>;
  if (status === "listed") return <Badge className="bg-green-500/20 text-green-400 border-green-500/30">Listed</Badge>;
  if (status === "draft") return <Badge className="bg-yellow-500/20 text-yellow-400 border-yellow-500/30">Draft</Badge>;
  return <Badge variant="outline">{status}</Badge>;
}

interface MetadataForm {
  title: string;
  description: string;
  tags: string;
}

function ListingReadiness({ file, isConnected }: { file: RawFile; isConnected: boolean }) {
  const meta = file.metadata as Record<string, unknown> | null;
  const hasTitle = !!(meta?.title);
  const hasDescription = !!(meta?.description);
  const hasTags = Array.isArray(meta?.tags) ? (meta.tags as string[]).length > 0 : false;
  const metadataComplete = hasTitle && hasDescription && hasTags;
  const hasPriceOrFree = file.price_cents != null && file.price_cents > 0;

  const checks = [
    { label: "File registered", passed: true },
    { label: "Metadata complete (title, description, tags)", passed: metadataComplete },
    { label: "Price set (if paid listing)", passed: hasPriceOrFree },
    { label: "Connected to ai.market (trust channel active)", passed: isConnected },
  ];

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Listing Readiness</CardTitle>
        <CardDescription>Complete these steps to publish your file</CardDescription>
      </CardHeader>
      <CardContent>
        <ul className="space-y-2">
          {checks.map((check) => (
            <li key={check.label} className="flex items-center gap-2 text-sm">
              {check.passed ? (
                <CheckCircle2 className="w-4 h-4 text-green-500 shrink-0" />
              ) : (
                <XCircle className="w-4 h-4 text-muted-foreground shrink-0" />
              )}
              <span className={check.passed ? "text-foreground" : "text-muted-foreground"}>
                {check.label}
              </span>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

function FilePreview({ file }: { file: RawFile }) {
  const mime = file.mime_type || "";
  const [objectUrl, setObjectUrl] = useState<string | null>(null);
  const [loadError, setLoadError] = useState(false);

  useEffect(() => {
    const needsPreview = mime.startsWith("image/") || mime.startsWith("audio/") || mime === "application/pdf";
    if (!needsPreview) return;

    let cancelled = false;
    let url: string | null = null;
    rawFilesApi.getFileObjectUrl(file.id).then((u) => {
      if (cancelled) { URL.revokeObjectURL(u); return; }
      url = u;
      setObjectUrl(u);
    }).catch(() => {
      if (!cancelled) setLoadError(true);
    });

    return () => {
      cancelled = true;
      if (url) URL.revokeObjectURL(url);
    };
  }, [file.id, mime]);

  if (mime.startsWith("image/")) {
    return (
      <div className="rounded-lg border bg-muted/50 p-4 flex items-center justify-center min-h-[200px]">
        {objectUrl && !loadError ? (
          <img
            src={objectUrl}
            alt={file.filename}
            className="max-h-[400px] max-w-full object-contain rounded"
            onError={() => setLoadError(true)}
          />
        ) : loadError ? (
          <>
            <FileImage className="w-16 h-16 text-muted-foreground" />
            <span className="ml-3 text-sm text-muted-foreground">Failed to load preview</span>
          </>
        ) : (
          <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
        )}
      </div>
    );
  }

  if (mime.startsWith("audio/")) {
    return (
      <div className="rounded-lg border bg-muted/50 p-4">
        <div className="flex items-center gap-3 mb-3">
          <FileAudio className="w-8 h-8 text-muted-foreground" />
          <span className="text-sm font-medium">{file.filename}</span>
        </div>
        {objectUrl && !loadError ? (
          <audio controls className="w-full" src={objectUrl} />
        ) : loadError ? (
          <span className="text-xs text-muted-foreground">Failed to load audio</span>
        ) : (
          <div className="h-10 flex items-center justify-center">
            <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
          </div>
        )}
      </div>
    );
  }

  if (mime === "application/pdf") {
    return (
      <div className="rounded-lg border bg-muted/50 p-4 min-h-[400px]">
        {objectUrl && !loadError ? (
          <object
            data={objectUrl}
            type="application/pdf"
            className="w-full h-[500px] rounded"
          >
            <p className="text-sm text-muted-foreground text-center py-8">
              PDF preview not supported in this browser.{" "}
              <button className="underline" onClick={() => rawFilesApi.downloadRawFile(file)}>Download</button> instead.
            </p>
          </object>
        ) : loadError ? (
          <div className="flex items-center justify-center min-h-[200px]">
            <FileText className="w-16 h-16 text-muted-foreground" />
            <span className="ml-3 text-sm text-muted-foreground">Failed to load PDF preview</span>
          </div>
        ) : (
          <div className="flex items-center justify-center min-h-[200px]">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
          </div>
        )}
      </div>
    );
  }

  const IconComponent = getFileIcon(file.mime_type);
  return (
    <div className="rounded-lg border bg-muted/50 p-4 flex items-center justify-center min-h-[200px]">
      <IconComponent className="w-16 h-16 text-muted-foreground" />
      <span className="ml-3 text-sm text-muted-foreground">{file.filename}</span>
    </div>
  );
}

export default function RawFileDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { isConnected } = useMode();
  const [file, setFile] = useState<RawFile | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [form, setForm] = useState<MetadataForm>({ title: "", description: "", tags: "" });

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    rawFilesApi.getRawFile(id)
      .then((data) => {
        setFile(data);
        const meta = data.metadata as Record<string, unknown> | null;
        setForm({
          title: (meta?.title as string) || "",
          description: (meta?.description as string) || "",
          tags: Array.isArray(meta?.tags) ? (meta.tags as string[]).join(", ") : "",
        });
      })
      .catch(() => {
        toast({ title: "Error", description: "Failed to load file details", variant: "destructive" });
      })
      .finally(() => setLoading(false));
  }, [id]);

  const handleSave = async () => {
    if (!id || !file) return;
    setSaving(true);
    try {
      const metadata = {
        ...(file.metadata || {}),
        title: form.title,
        description: form.description,
        tags: form.tags.split(",").map((t) => t.trim()).filter(Boolean),
      };
      const updated = await rawFilesApi.updateRawFile(id, metadata);
      setFile(updated);
      toast({ title: "Saved", description: "Metadata updated successfully" });
    } catch {
      toast({ title: "Error", description: "Failed to save metadata", variant: "destructive" });
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!id || !confirm("Are you sure you want to delete this file?")) return;
    setDeleting(true);
    try {
      await rawFilesApi.deleteRawFile(id);
      toast({ title: "Deleted", description: "File has been deleted" });
      navigate("/datasets");
    } catch {
      toast({ title: "Error", description: "Failed to delete file", variant: "destructive" });
      setDeleting(false);
    }
  };

  const handleCreateListing = () => {
    if (!file) return;
    const meta = file.metadata as Record<string, unknown> | null;
    const title = (meta?.title as string) || file.filename;
    const description = (meta?.description as string) || "";
    const tags = Array.isArray(meta?.tags) ? (meta.tags as string[]) : [];
    navigate("/ai-market", { state: { createListing: { rawFileId: file.id, title, description, tags } } });
  };

  const handleVectorize = () => {
    if (!file) return;
    toast({
      title: "Coming Soon",
      description: "Vectorization from raw files is not yet available. Upload the file as a dataset to vectorize it.",
    });
  };

  if (loading) {
    return (
      <div className="container mx-auto py-6 max-w-4xl space-y-6">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-[200px] w-full" />
        <Skeleton className="h-[300px] w-full" />
      </div>
    );
  }

  if (!file) {
    return (
      <div className="container mx-auto py-6 max-w-4xl">
        <p className="text-muted-foreground">File not found.</p>
        <Button variant="ghost" asChild className="mt-4">
          <Link to="/datasets"><ArrowLeft className="w-4 h-4 mr-2" />Back to Datasets</Link>
        </Button>
      </div>
    );
  }

  return (
    <div className="container mx-auto py-6 max-w-4xl space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" asChild>
            <Link to="/datasets"><ArrowLeft className="w-4 h-4" /></Link>
          </Button>
          <div>
            <h1 className="text-xl font-semibold">{file.filename}</h1>
            <p className="text-sm text-muted-foreground">
              {formatBytes(file.file_size_bytes)} &middot; {file.mime_type || "Unknown type"} &middot; {getListingStatusBadge(file.listing_status)}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={async () => {
              try {
                await rawFilesApi.downloadRawFile(file);
              } catch {
                toast({ title: "Error", description: "Failed to download file", variant: "destructive" });
              }
            }}
          >
            <Download className="w-4 h-4 mr-1" />Download
          </Button>
          <Button variant="destructive" size="sm" onClick={handleDelete} disabled={deleting}>
            {deleting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4 mr-1" />}
            Delete
          </Button>
        </div>
      </div>

      {/* File Preview */}
      <FilePreview file={file} />

      {/* Metadata Editor */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Metadata</CardTitle>
          <CardDescription>Edit file metadata for marketplace listing</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="meta-title">Title</Label>
            <Input
              id="meta-title"
              placeholder="Enter a title for this file"
              value={form.title}
              onChange={(e) => setForm((f) => ({ ...f, title: e.target.value }))}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="meta-description">Description</Label>
            <Input
              id="meta-description"
              placeholder="Describe the contents and potential use cases"
              value={form.description}
              onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="meta-tags">Tags (comma-separated)</Label>
            <Input
              id="meta-tags"
              placeholder="e.g. finance, csv, quarterly-report"
              value={form.tags}
              onChange={(e) => setForm((f) => ({ ...f, tags: e.target.value }))}
            />
          </div>

          {/* Show allAI-extracted technical metadata if present */}
          {file.metadata && (file.metadata as Record<string, unknown>).technical_metadata && (
            <div className="pt-2">
              <Label className="text-muted-foreground text-xs">Technical Metadata (auto-extracted)</Label>
              <pre className="mt-1 text-xs bg-muted p-2 rounded overflow-auto max-h-32">
                {JSON.stringify((file.metadata as Record<string, unknown>).technical_metadata, null, 2)}
              </pre>
            </div>
          )}

          <Button onClick={handleSave} disabled={saving} size="sm">
            {saving ? <Loader2 className="w-4 h-4 mr-1 animate-spin" /> : <Save className="w-4 h-4 mr-1" />}
            Save Metadata
          </Button>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Listing Readiness */}
        <ListingReadiness file={file} isConnected={isConnected} />

        {/* Vectorize CTA */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base flex items-center gap-2">
              <Zap className="w-4 h-4" />
              Vectorize for AI Queries
            </CardTitle>
            <CardDescription>
              Enable semantic search, RAG, and ai_queryable fulfillment by vectorizing this file.
              After vectorization, your listing can be upgraded from direct download to AI-queryable.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={handleVectorize} className="w-full">
              <Zap className="w-4 h-4 mr-2" />
              Vectorize This File
            </Button>
          </CardContent>
        </Card>
      </div>

      {/* Actions */}
      <Separator />
      <div className="flex gap-3">
        <Button onClick={handleCreateListing} variant="default">
          <ShoppingCart className="w-4 h-4 mr-2" />
          Create Listing
        </Button>
      </div>
    </div>
  );
}
