import { createContext, useContext, useState, useCallback, useRef, useEffect } from "react";
import { datasetsApi, DuplicateFileError, UploadAbortedError, importApi } from "@/lib/api";
import { toast } from "sonner";

// ---------------------------------------------------------------------------
// Types (exported so FileUploadModal & UploadIndicator can use them)
// ---------------------------------------------------------------------------

export type FileState = "pending" | "uploading" | "processing" | "complete" | "error" | "duplicate" | "rejected";

export interface QueuedFile {
  id: string;
  file: File;
  relativePath: string | null;
  state: FileState;
  progress: number;
  datasetId: string | null;
  error: string | null;
  existingDatasetId: string | null;
  /** Backend queue position — reported by FileRow via onMetadataUpdate */
  queuePosition: number | null;
  /** Processing phase: queued | extracting | indexing | null */
  processingPhase: string | null;
}

// ---------------------------------------------------------------------------
// Context shape
// ---------------------------------------------------------------------------

interface UploadContextValue {
  // Queue state
  queue: QueuedFile[];
  addFiles: (files: File[], relativePaths?: (string | undefined)[]) => void;
  removeFile: (id: string) => void;
  updateFile: (id: string, updates: Partial<QueuedFile>) => void;
  cancelFile: (id: string) => void;
  handleStatusChange: (id: string, state: FileState, error?: string) => void;
  handleMetadataUpdate: (id: string, phase: string | null, queuePosition: number | null) => void;

  // Upload actions
  isUploading: boolean;
  handleUploadAll: () => Promise<void>;
  handleUploadDuplicates: () => Promise<void>;
  handleSkipDuplicates: () => void;

  // Modal state
  isModalOpen: boolean;
  openModal: () => void;
  closeModal: () => void;

  // Local import state
  isImporting: boolean;
  setIsImporting: (v: boolean) => void;
  showLocalImport: boolean;
  setShowLocalImport: (v: boolean) => void;
  hasImportFiles: boolean | null;

  // Large-batch confirmation
  showLargeWarning: boolean;
  setShowLargeWarning: (v: boolean) => void;

  // Derived booleans
  hasFiles: boolean;
  hasPending: boolean;
  hasDuplicates: boolean;
  isProcessing: boolean;
  allDone: boolean;
  hasFailures: boolean;

  // onSuccess callback (triggers dataset list refresh)
  onSuccess: () => void;
  setOnSuccess: (cb: () => void) => void;
}

const UploadContext = createContext<UploadContextValue | null>(null);

export function useUpload() {
  const ctx = useContext(UploadContext);
  if (!ctx) throw new Error("useUpload must be used within UploadProvider");
  return ctx;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LARGE_BATCH_WARNING_BYTES = 50 * 1024 * 1024 * 1024; // 50 GB
const JUNK_FILES = new Set([".DS_Store", "Thumbs.db", ".gitkeep", ".gitignore", "desktop.ini"]);

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export function UploadProvider({ children }: { children: React.ReactNode }) {
  // ----- queue -----
  const [queue, setQueue] = useState<QueuedFile[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const abortHandles = useRef<Map<string, () => void>>(new Map());

  // ----- modal -----
  const [isModalOpen, setIsModalOpen] = useState(false);

  // ----- local import -----
  const [isImporting, setIsImporting] = useState(false);
  const [showLocalImport, setShowLocalImport] = useState(false);
  const [hasImportFiles, setHasImportFiles] = useState<boolean | null>(null);

  // ----- large batch warning -----
  const [showLargeWarning, setShowLargeWarning] = useState(false);

  // ----- onSuccess callback (set by consumers to refresh dataset lists) -----
  const onSuccessRef = useRef<() => void>(() => {});
  const setOnSuccess = useCallback((cb: () => void) => {
    onSuccessRef.current = cb;
  }, []);
  const onSuccess = useCallback(() => {
    onSuccessRef.current();
  }, []);

  // ----- derived -----
  const hasFiles = queue.length > 0;
  const hasPending = queue.some((f) => f.state === "pending");
  const hasDuplicates = queue.some((f) => f.state === "duplicate");
  const isProcessing = queue.some((f) => f.state === "processing");
  const allDone = hasFiles && queue.every((f) => f.state === "complete" || f.state === "error" || f.state === "rejected");
  const hasFailures = hasFiles && queue.some((f) => f.state === "error" || f.state === "rejected");

  // ----- Check for server import files when modal opens -----
  useEffect(() => {
    if (!isModalOpen) return;
    let cancelled = false;
    importApi.browse("", 1, 0).then((res) => {
      if (!cancelled) setHasImportFiles(res.entries.length > 0);
    }).catch(() => {
      if (!cancelled) setHasImportFiles(false);
    });
    return () => { cancelled = true; };
  }, [isModalOpen]);

  // ----- queue manipulation -----
  const addFiles = useCallback((files: File[], relativePaths?: (string | undefined)[]) => {
    setQueue((prev) => {
      const newItems: QueuedFile[] = [];
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        if (JUNK_FILES.has(file.name)) continue;
        const relPath = relativePaths?.[i] ?? null;
        newItems.push({
          id: `${file.name}-${file.size}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          file,
          relativePath: relPath,
          state: "pending",
          progress: 0,
          datasetId: null,
          error: null,
          existingDatasetId: null,
          queuePosition: null,
          processingPhase: null,
        });
      }
      return [...prev, ...newItems];
    });
  }, []);

  const removeFile = useCallback((id: string) => {
    setQueue((prev) => prev.filter((f) => f.id !== id));
  }, []);

  const updateFile = useCallback((id: string, updates: Partial<QueuedFile>) => {
    setQueue((prev) => prev.map((f) => (f.id === id ? { ...f, ...updates } : f)));
  }, []);

  const handleStatusChange = useCallback((id: string, state: FileState, error?: string) => {
    updateFile(id, { state, error: error ?? null });
  }, [updateFile]);

  const handleMetadataUpdate = useCallback((id: string, phase: string | null, queuePosition: number | null) => {
    setQueue((prev) => prev.map((f) =>
      f.id === id ? { ...f, processingPhase: phase, queuePosition } : f
    ));
  }, []);

  const cancelFile = useCallback((id: string) => {
    setQueue((prev) => {
      const item = prev.find((f) => f.id === id);
      if (!item) return prev;

      if (item.state === "uploading") {
        const abort = abortHandles.current.get(id);
        if (abort) abort();
        abortHandles.current.delete(id);
        return prev.map((f) => f.id === id ? { ...f, state: "error" as FileState, error: "Cancelled" } : f);
      }

      if (item.state === "processing" && item.datasetId) {
        // Fire-and-forget backend cancel
        datasetsApi.delete(item.datasetId).catch(() => {});
        return prev.map((f) => f.id === id ? { ...f, state: "error" as FileState, error: "Cancelled" } : f);
      }

      return prev;
    });
  }, []);

  // ----- batch -----
  const batchIdRef = useRef<string | null>(null);

  const concurrentUploads = (() => {
    const stored = localStorage.getItem("vectoraiz_concurrent_uploads");
    if (stored && stored !== "auto") {
      const n = parseInt(stored, 10);
      if (n >= 1 && n <= 6) return n;
    }
    const rec = localStorage.getItem("vectoraiz_recommended_concurrent");
    const n = parseInt(rec || "", 10);
    return n >= 1 && n <= 6 ? n : 3;
  })();

  async function runWithConcurrency<T>(
    items: T[],
    fn: (item: T) => Promise<unknown>,
    concurrency: number,
  ): Promise<void> {
    const q = [...items];
    const workers = Array.from({ length: Math.min(concurrency, q.length) }, async () => {
      while (q.length > 0) {
        const item = q.shift()!;
        await fn(item);
      }
    });
    await Promise.all(workers);
  }

  const uploadOne = async (item: QueuedFile, allowDuplicate: boolean) => {
    updateFile(item.id, { state: "uploading", progress: 0 });
    try {
      const { promise, abort } = datasetsApi.uploadWithProgress(item.file, {
        allowDuplicate,
        batchId: batchIdRef.current ?? undefined,
        onProgress: (pct) => updateFile(item.id, { progress: pct }),
      });
      abortHandles.current.set(item.id, abort);
      const result = await promise;
      abortHandles.current.delete(item.id);
      updateFile(item.id, { state: "processing", progress: 100, datasetId: result.dataset_id });
      return "ok";
    } catch (e) {
      abortHandles.current.delete(item.id);
      if (e instanceof UploadAbortedError) {
        // cancelFile already set the state — just return
        return "cancelled";
      }
      if (e instanceof DuplicateFileError) {
        updateFile(item.id, { state: "duplicate", progress: 0, existingDatasetId: e.existingDataset.id, error: null });
        return "duplicate";
      }
      updateFile(item.id, { state: "error", error: e instanceof Error ? e.message : "Upload failed" });
      return "error";
    }
  };

  const sendBatchSummary = useCallback(async (q: QueuedFile[]) => {
    const bid = batchIdRef.current;
    if (!bid) return;
    const ok = q.filter((f) => f.state === "complete" || f.state === "processing").length;
    const fail = q.filter((f) => f.state === "error" || f.state === "rejected").length;
    if (ok + fail < 2) return;
    const failedNames = q.filter((f) => f.state === "error" || f.state === "rejected").map((f) => f.file.name);
    try {
      await datasetsApi.uploadBatchSummary(bid, ok, fail, failedNames);
    } catch {
      // best-effort
    }
  }, []);

  const handleUploadAll = useCallback(async () => {
    const pending = queue.filter((f) => f.state === "pending");
    const totalPendingSize = pending.reduce((sum, f) => sum + f.file.size, 0);
    if (totalPendingSize > LARGE_BATCH_WARNING_BYTES && !showLargeWarning) {
      setShowLargeWarning(true);
      return;
    }
    setShowLargeWarning(false);
    setIsUploading(true);
    batchIdRef.current = `upl_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
    await runWithConcurrency(pending, (item) => uploadOne(item, false), concurrentUploads);
    setIsUploading(false);
  }, [queue, showLargeWarning, concurrentUploads]);

  const handleUploadDuplicates = useCallback(async () => {
    setIsUploading(true);
    const dupes = queue.filter((f) => f.state === "duplicate");
    await runWithConcurrency(dupes, (item) => uploadOne(item, true), concurrentUploads);
    setIsUploading(false);
  }, [queue, concurrentUploads]);

  const handleSkipDuplicates = useCallback(() => {
    setQueue((prev) => prev.filter((f) => f.state !== "duplicate"));
  }, []);

  // ----- poll processing items even when modal is closed -----
  const processingKey = queue
    .filter((f) => f.state === "processing" && f.datasetId)
    .map((f) => f.datasetId)
    .join(",");

  useEffect(() => {
    if (!processingKey) return;

    const interval = setInterval(async () => {
      const processingItems = queue.filter((f) => f.state === "processing" && f.datasetId);
      for (const item of processingItems) {
        if (!item.datasetId) continue;
        try {
          const data = await datasetsApi.getStatus(item.datasetId);
          if (data.status === "ready" || data.status === "preview_ready") {
            handleStatusChange(item.id, "complete");
          } else if (data.status === "error") {
            handleStatusChange(item.id, "error", data.error || "Processing failed");
          }
          // Update phase/queuePosition metadata for the indicator
          const raw = data as Record<string, unknown>;
          if (raw.phase !== undefined || raw.queue_position !== undefined) {
            handleMetadataUpdate(
              item.id,
              (raw.phase as string) ?? null,
              (raw.queue_position as number) ?? null,
            );
          }
        } catch {
          // Ignore network errors, retry next interval
        }
      }
    }, 3000);

    return () => clearInterval(interval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [processingKey]);

  // ----- allDone effect: summary toast + auto-clear -----
  useEffect(() => {
    if (!allDone || queue.length === 0) return;

    const ok = queue.filter((f) => f.state === "complete").length;
    const cancelled = queue.filter((f) => f.state === "error" && f.error === "Cancelled").length;
    const fail = queue.filter((f) => f.state === "error" && f.error !== "Cancelled").length;
    const skipped = queue.filter((f) => f.state === "rejected").length;

    sendBatchSummary(queue);

    const parts: string[] = [];
    if (ok > 0) parts.push(`${ok} succeeded`);
    if (fail > 0) parts.push(`${fail} failed`);
    if (cancelled > 0) parts.push(`${cancelled} cancelled`);
    if (skipped > 0) parts.push(`${skipped} skipped`);

    if (fail > 0 || skipped > 0) {
      if (ok > 0) toast.warning(parts.join(", ")); else toast.error(parts.join(", "));
      if (ok > 0) onSuccess();
    } else {
      toast.success(parts.join(", "));
      onSuccess();
      // Auto-clear after 5 seconds if modal is not open (indicator will show final status)
      const timer = setTimeout(() => {
        // Only auto-clear if modal is closed — if modal is open, let user close manually
        setQueue((prev) => {
          const stillAllDone = prev.length > 0 && prev.every((f) => f.state === "complete" || f.state === "error" || f.state === "rejected");
          const noFailures = !prev.some((f) => f.state === "error" || f.state === "rejected");
          if (stillAllDone && noFailures) return [];
          return prev;
        });
      }, 5000);
      return () => clearTimeout(timer);
    }
  }, [allDone]);

  // ----- modal -----
  const openModal = useCallback(() => {
    // Auto-reset stale completed results when reopening
    setQueue((prev) => {
      const allFinished = prev.length > 0 && prev.every((f) => f.state === "complete" || f.state === "error" || f.state === "rejected");
      return allFinished ? [] : prev;
    });
    setIsModalOpen(true);
  }, []);

  const closeModal = useCallback(() => {
    // If uploads/imports are active, just hide — don't destroy state
    if (isUploading || isImporting || isProcessing) {
      setIsModalOpen(false);
      if (isUploading || isProcessing) {
        toast.info("Uploads continue in the background");
      }
      return;
    }
    // If all done with no failures, clear queue on close
    if (allDone && !hasFailures) {
      setQueue([]);
    }
    // If all done WITH failures, keep queue so user can reopen and review
    // If queue is empty or only has pending files and user closes, clear pending
    if (!allDone && !hasFiles) {
      // nothing to clear
    }
    setShowLocalImport(false);
    setIsModalOpen(false);
  }, [isUploading, isImporting, isProcessing, allDone, hasFailures, hasFiles]);

  // ----- context value -----
  const value: UploadContextValue = {
    queue,
    addFiles,
    removeFile,
    updateFile,
    cancelFile,
    handleStatusChange,
    handleMetadataUpdate,
    isUploading,
    handleUploadAll,
    handleUploadDuplicates,
    handleSkipDuplicates,
    isModalOpen,
    openModal,
    closeModal,
    isImporting,
    setIsImporting,
    showLocalImport,
    setShowLocalImport,
    hasImportFiles,
    showLargeWarning,
    setShowLargeWarning,
    hasFiles,
    hasPending,
    hasDuplicates,
    isProcessing,
    allDone,
    hasFailures,
    onSuccess,
    setOnSuccess,
  };

  return <UploadContext.Provider value={value}>{children}</UploadContext.Provider>;
}
