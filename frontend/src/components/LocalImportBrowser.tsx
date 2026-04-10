import { useState, useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import {
  Folder,
  File,
  FileSpreadsheet,
  FileJson,
  FileText,
  Database,
  ChevronRight,
  ArrowLeft,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  HardDrive,
  Download,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  importApi,
  type ImportBrowseEntry,
  type ImportProgress,
  type ImportFileResult,
} from "@/lib/api";
import { useBrand } from "@/contexts/BrandContext";
import { toast } from "sonner";

type Phase = "browse" | "importing" | "complete";

interface LocalImportBrowserProps {
  onImportingChange?: (importing: boolean) => void;
  onSuccess?: () => void;
  onClose?: () => void;
}

const formatBytes = (bytes: number): string => {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
};

const getFileIcon = (name: string) => {
  const ext = name.split(".").pop()?.toLowerCase();
  switch (ext) {
    case "csv":
    case "tsv":
    case "xlsx":
    case "xls":
      return FileSpreadsheet;
    case "json":
      return FileJson;
    case "pdf":
    case "doc":
    case "docx":
      return FileText;
    case "parquet":
      return Database;
    default:
      return File;
  }
};

export function LocalImportBrowser({
  onImportingChange,
  onSuccess,
  onClose,
}: LocalImportBrowserProps) {
  const { importDir, importDirEnvVar } = useBrand();

  // Browse state
  const [rootPath, setRootPath] = useState("");
  const [currentPath, setCurrentPath] = useState("");
  const [entries, setEntries] = useState<ImportBrowseEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [browseError, setBrowseError] = useState<string | null>(null);
  const [selectedFiles, setSelectedFiles] = useState<Set<string>>(new Set());

  // Import state
  const [phase, setPhase] = useState<Phase>("browse");
  const [jobId, setJobId] = useState<string | null>(null);
  const [progress, setProgress] = useState<ImportProgress | null>(null);
  const [results, setResults] = useState<ImportFileResult[]>([]);
  const [importError, setImportError] = useState<string | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadDirectory = async (path: string, isInitial = false) => {
    setLoading(true);
    setBrowseError(null);
    setSelectedFiles(new Set());
    try {
      const res = await importApi.browse(path);
      if (isInitial) setRootPath(res.path);
      setCurrentPath(res.path);
      setEntries(res.entries);
    } catch (e) {
      setBrowseError(
        e instanceof Error ? e.message : "Failed to browse directory"
      );
      setEntries([]);
    } finally {
      setLoading(false);
    }
  };

  // Initial load
  useEffect(() => {
    loadDirectory("", true);
  }, []);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  // Navigation
  const navigateInto = (dirName: string) => {
    const sep = currentPath.endsWith("/") ? "" : "/";
    loadDirectory(currentPath + sep + dirName + "/");
  };

  const navigateUp = () => {
    const trimmed = currentPath.replace(/\/$/, "");
    const parent = trimmed.substring(0, trimmed.lastIndexOf("/") + 1);
    if (parent && parent.length >= rootPath.length) {
      loadDirectory(parent);
    }
  };

  const navigateToBreadcrumb = (path: string) => {
    loadDirectory(path);
  };

  // Selection
  const toggleFile = (name: string) => {
    setSelectedFiles((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const fileEntries = entries.filter((e) => e.type === "file");
  const dirEntries = entries.filter((e) => e.type === "directory");

  const selectAll = () => {
    setSelectedFiles(new Set(fileEntries.map((e) => e.name)));
  };

  const deselectAll = () => {
    setSelectedFiles(new Set());
  };

  const selectedSize = fileEntries
    .filter((e) => selectedFiles.has(e.name))
    .reduce((sum, e) => sum + (e.size_bytes || 0), 0);

  // Breadcrumbs
  const breadcrumbs = (() => {
    if (!currentPath || !rootPath) return [];
    const relative = currentPath.slice(rootPath.length).replace(/\/$/, "");
    if (!relative) return [{ label: "imports", path: rootPath }];
    const parts = relative.split("/").filter(Boolean);
    const crumbs = [{ label: "imports", path: rootPath }];
    let accumulated = rootPath;
    for (const part of parts) {
      accumulated += part + "/";
      crumbs.push({ label: part, path: accumulated });
    }
    return crumbs;
  })();

  // Start import
  const handleImport = async () => {
    if (selectedFiles.size === 0) return;

    const relativeBase = currentPath.slice(rootPath.length);
    const filePaths = Array.from(selectedFiles).map(
      (name) => relativeBase + name
    );

    setPhase("importing");
    onImportingChange?.(true);
    setImportError(null);

    try {
      const res = await importApi.start(rootPath, filePaths);
      setJobId(res.job_id);

      // Start polling
      pollRef.current = setInterval(async () => {
        try {
          const status = await importApi.getStatus(res.job_id);
          setProgress(status.progress);
          setResults(status.results);

          if (
            status.status === "complete" ||
            status.status === "failed" ||
            status.status === "cancelled"
          ) {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = null;
            setPhase("complete");
            onImportingChange?.(false);

            if (status.status === "complete") {
              const ok = status.results.filter(
                (r) => r.status === "complete" || r.status === "done"
              ).length;
              toast.success(
                `Imported ${ok} dataset${ok !== 1 ? "s" : ""}`
              );
              onSuccess?.();
            } else if (status.status === "cancelled") {
              toast.warning("Import cancelled");
            } else {
              toast.error("Import failed");
            }
          }
        } catch {
          // Polling error — keep trying
        }
      }, 1000);
    } catch (e) {
      setImportError(
        e instanceof Error ? e.message : "Failed to start import"
      );
      setPhase("browse");
      onImportingChange?.(false);
    }
  };

  // Cancel import
  const handleCancel = async () => {
    if (!jobId) return;
    try {
      await importApi.cancel(jobId);
    } catch {
      // Best effort
    }
  };

  // Reset to browse
  const handleReset = () => {
    setPhase("browse");
    setJobId(null);
    setProgress(null);
    setResults([]);
    setImportError(null);
    loadDirectory(currentPath);
  };

  // ── Browse phase ──
  if (phase === "browse") {
    return (
      <div className="space-y-3">
        {/* Breadcrumbs */}
        {!loading && !browseError && (
          <div className="flex items-center gap-1 text-sm text-muted-foreground min-h-[28px]">
            {currentPath !== rootPath && (
              <button
                onClick={navigateUp}
                className="p-1 rounded hover:bg-secondary transition-colors"
              >
                <ArrowLeft className="w-3.5 h-3.5" />
              </button>
            )}
            {breadcrumbs.map((crumb, i) => (
              <span key={crumb.path} className="flex items-center gap-1">
                {i > 0 && (
                  <ChevronRight className="w-3 h-3 text-muted-foreground/50" />
                )}
                <button
                  onClick={() => navigateToBreadcrumb(crumb.path)}
                  className={cn(
                    "hover:text-foreground transition-colors px-1 rounded",
                    i === breadcrumbs.length - 1 &&
                      "text-foreground font-medium"
                  )}
                >
                  {crumb.label}
                </button>
              </span>
            ))}
          </div>
        )}

        {/* Loading */}
        {loading && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-5 h-5 text-primary animate-spin" />
          </div>
        )}

        {/* Error or empty state */}
        {!loading && (browseError || entries.length === 0) && (
          <div className="flex flex-col items-center gap-3 py-8 text-center">
            <div className="w-12 h-12 rounded-full bg-secondary flex items-center justify-center">
              <HardDrive className="w-6 h-6 text-muted-foreground" />
            </div>
            <div className="space-y-1">
              <p className="text-sm text-foreground font-medium">
                No files found in import directory.
              </p>
              <p className="text-xs text-muted-foreground leading-relaxed">
                Add files to {importDir} on your machine,
                <br />
                or set {importDirEnvVar} in your .env file.
              </p>
            </div>
          </div>
        )}

        {/* File list */}
        {!loading && !browseError && entries.length > 0 && (
          <>
            {/* Select all / deselect all */}
            {fileEntries.length > 0 && (
              <div className="flex items-center gap-2 text-xs">
                <button
                  onClick={
                    selectedFiles.size === fileEntries.length
                      ? deselectAll
                      : selectAll
                  }
                  className="text-primary hover:text-primary/80 transition-colors"
                >
                  {selectedFiles.size === fileEntries.length
                    ? "Deselect All"
                    : "Select All"}
                </button>
                {selectedFiles.size > 0 && (
                  <span className="text-muted-foreground">
                    {selectedFiles.size} file
                    {selectedFiles.size !== 1 ? "s" : ""} (
                    {formatBytes(selectedSize)})
                  </span>
                )}
              </div>
            )}

            <div className="space-y-1 max-h-56 overflow-y-auto">
              {/* Directories first */}
              {dirEntries.map((entry) => (
                <button
                  key={entry.name}
                  onClick={() => navigateInto(entry.name)}
                  className="flex items-center gap-3 w-full px-3 py-2 rounded-lg border border-border hover:border-primary/30 hover:bg-secondary/50 transition-colors text-left"
                >
                  <Folder className="w-4 h-4 text-primary flex-shrink-0" />
                  <span className="text-sm text-foreground truncate flex-1">
                    {entry.name}
                  </span>
                  <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
                </button>
              ))}

              {/* Files */}
              {fileEntries.map((entry) => {
                const Icon = getFileIcon(entry.name);
                const isSelected = selectedFiles.has(entry.name);
                return (
                  <button
                    key={entry.name}
                    onClick={() => toggleFile(entry.name)}
                    className={cn(
                      "flex items-center gap-3 w-full px-3 py-2 rounded-lg border transition-colors text-left",
                      isSelected
                        ? "border-primary/40 bg-primary/5"
                        : "border-border hover:border-border hover:bg-secondary/30"
                    )}
                  >
                    <div
                      className={cn(
                        "w-4 h-4 rounded border flex items-center justify-center flex-shrink-0 transition-colors",
                        isSelected
                          ? "bg-primary border-primary"
                          : "border-muted-foreground/40"
                      )}
                    >
                      {isSelected && (
                        <svg
                          className="w-3 h-3 text-primary-foreground"
                          fill="none"
                          viewBox="0 0 24 24"
                          stroke="currentColor"
                          strokeWidth={3}
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            d="M5 13l4 4L19 7"
                          />
                        </svg>
                      )}
                    </div>
                    <Icon className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                    <span className="text-sm text-foreground truncate flex-1">
                      {entry.name}
                    </span>
                    {entry.size_bytes != null && (
                      <span className="text-xs text-muted-foreground flex-shrink-0">
                        {formatBytes(entry.size_bytes)}
                      </span>
                    )}
                  </button>
                );
              })}
            </div>
          </>
        )}

        {/* Import error */}
        {importError && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-destructive/30 bg-destructive/5">
            <XCircle className="w-4 h-4 text-destructive flex-shrink-0" />
            <p className="text-xs text-destructive">{importError}</p>
          </div>
        )}

        {/* Footer actions */}
        <div className="flex items-center justify-between pt-1">
          <Button variant="ghost" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button
            size="sm"
            disabled={selectedFiles.size === 0}
            onClick={handleImport}
            className="gap-2"
          >
            <Download className="w-3.5 h-3.5" />
            Import Selected ({selectedFiles.size})
          </Button>
        </div>
      </div>
    );
  }

  // ── Importing phase ──
  if (phase === "importing") {
    const overallPct = progress
      ? progress.bytes_total > 0
        ? Math.round((progress.bytes_copied / progress.bytes_total) * 100)
        : 0
      : 0;

    return (
      <div className="space-y-4 py-2">
        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-foreground font-medium">
              Importing files…
            </span>
            <span className="text-muted-foreground">{overallPct}%</span>
          </div>
          <Progress value={overallPct} className="h-2" />
          {progress && (
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>
                {progress.files_complete} / {progress.files_total} files
              </span>
              <span>
                {formatBytes(progress.bytes_copied)} /{" "}
                {formatBytes(progress.bytes_total)}
              </span>
            </div>
          )}
        </div>

        {/* Current file */}
        {progress?.current_file && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-primary/20 bg-primary/5">
            <Loader2 className="w-3.5 h-3.5 text-primary animate-spin flex-shrink-0" />
            <span className="text-xs text-foreground truncate">
              {progress.current_file}
            </span>
            {progress.current_file_pct != null && (
              <span className="text-xs text-muted-foreground flex-shrink-0">
                {Math.round(progress.current_file_pct)}%
              </span>
            )}
          </div>
        )}

        {/* Per-file status list */}
        {results.length > 0 && (
          <div className="space-y-1 max-h-40 overflow-y-auto">
            {results.map((r) => (
              <div
                key={r.file}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded text-xs",
                  (r.status === "complete" || r.status === "done") &&
                    "text-green-400",
                  r.status === "error" && "text-destructive",
                  r.status === "copying" && "text-primary",
                  (r.status === "pending" || r.status === "processing") &&
                    "text-muted-foreground"
                )}
              >
                {(r.status === "complete" || r.status === "done") && (
                  <CheckCircle2 className="w-3 h-3 flex-shrink-0" />
                )}
                {r.status === "error" && (
                  <XCircle className="w-3 h-3 flex-shrink-0" />
                )}
                {r.status === "copying" && (
                  <Loader2 className="w-3 h-3 animate-spin flex-shrink-0" />
                )}
                {(r.status === "pending" || r.status === "processing") && (
                  <File className="w-3 h-3 flex-shrink-0" />
                )}
                <span className="truncate">{r.file}</span>
              </div>
            ))}
          </div>
        )}

        <div className="flex justify-end">
          <Button variant="destructive" size="sm" onClick={handleCancel}>
            Cancel Import
          </Button>
        </div>
      </div>
    );
  }

  // ── Complete phase ──
  const successCount = results.filter(
    (r) => r.status === "complete" || r.status === "done"
  ).length;
  const failCount = results.filter((r) => r.status === "error").length;

  return (
    <div className="space-y-4 py-2">
      <div className="flex flex-col items-center gap-3 py-4">
        {failCount === 0 ? (
          <CheckCircle2 className="w-10 h-10 text-green-500" />
        ) : successCount === 0 ? (
          <XCircle className="w-10 h-10 text-destructive" />
        ) : (
          <AlertTriangle className="w-10 h-10 text-yellow-500" />
        )}
        <div className="text-center space-y-1">
          <p className="text-sm font-medium text-foreground">
            {failCount === 0
              ? `Successfully imported ${successCount} dataset${successCount !== 1 ? "s" : ""}`
              : successCount === 0
                ? "Import failed"
                : `Imported ${successCount} dataset${successCount !== 1 ? "s" : ""}, ${failCount} failed`}
          </p>
          {progress && (
            <p className="text-xs text-muted-foreground">
              {formatBytes(progress.bytes_copied)} copied
            </p>
          )}
        </div>
      </div>

      {/* Failed files */}
      {failCount > 0 && (
        <div className="space-y-1 max-h-32 overflow-y-auto">
          {results
            .filter((r) => r.status === "error")
            .map((r) => (
              <div
                key={r.file}
                className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-destructive/20 bg-destructive/5 text-xs"
              >
                <XCircle className="w-3 h-3 text-destructive flex-shrink-0" />
                <span className="truncate text-foreground">{r.file}</span>
                {r.error && (
                  <span className="text-destructive flex-shrink-0">
                    — {r.error}
                  </span>
                )}
              </div>
            ))}
        </div>
      )}

      <div className="flex items-center justify-between">
        <Button variant="ghost" size="sm" onClick={handleReset}>
          Import More
        </Button>
        <Button size="sm" onClick={onClose}>
          Done
        </Button>
      </div>
    </div>
  );
}
