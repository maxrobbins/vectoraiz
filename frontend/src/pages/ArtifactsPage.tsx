/**
 * ArtifactsPage — View, download, and manage allAI-generated output files.
 *
 * PHASE: BQ-VZ-ARTIFACTS Phase 1
 * CREATED: 2026-03-06
 */

import { useEffect, useState, useCallback } from "react";
import { Star, Download, Trash2, FileText, FileSpreadsheet, FileCode, FileType, Globe, Eye, X } from "lucide-react";
import { artifactsApi, type ApiArtifact } from "@/lib/api";

const FORMAT_BADGES: Record<string, { label: string; className: string; icon: React.ComponentType<{ className?: string }> }> = {
  txt: { label: "TXT", className: "bg-blue-500/10 text-blue-400", icon: FileText },
  csv: { label: "CSV", className: "bg-green-500/10 text-green-400", icon: FileSpreadsheet },
  json: { label: "JSON", className: "bg-yellow-500/10 text-yellow-400", icon: FileCode },
  md: { label: "MD", className: "bg-purple-500/10 text-purple-400", icon: FileText },
  html: { label: "HTML", className: "bg-orange-500/10 text-orange-400", icon: Globe },
};

const SOURCE_LABELS: Record<string, string> = {
  "allai-copilot": "allAI Copilot",
  "allai-query": "SQL Export",
  "allai-agent": "allAI Agent",
  "manual": "Manual",
};

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatRelativeTime(isoDate: string): string {
  const diff = Date.now() - new Date(isoDate).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export default function ArtifactsPage() {
  const [artifacts, setArtifacts] = useState<ApiArtifact[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<{ artifact: ApiArtifact; content: string } | null>(null);

  const fetchArtifacts = useCallback(async () => {
    try {
      setLoading(true);
      const resp = await artifactsApi.list({ limit: 100 });
      setArtifacts(resp.artifacts);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load artifacts");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchArtifacts();
  }, [fetchArtifacts]);

  const handleStar = async (id: string, starred: boolean) => {
    try {
      const updated = await artifactsApi.star(id, starred);
      setArtifacts((prev) => prev.map((a) => (a.id === id ? updated : a)));
    } catch {}
  };

  const handleDelete = async (id: string) => {
    try {
      await artifactsApi.delete(id);
      setArtifacts((prev) => prev.filter((a) => a.id !== id));
      if (preview?.artifact.id === id) setPreview(null);
    } catch {}
  };

  const handleDownload = (id: string) => {
    const url = artifactsApi.downloadUrl(id);
    const apiKey = localStorage.getItem("vectoraiz_api_key");
    // Use a hidden form to send the API key header via fetch+blob
    fetch(url, { headers: apiKey ? { "X-API-Key": apiKey } : {} })
      .then((r) => r.blob())
      .then((blob) => {
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        const artifact = artifacts.find((art) => art.id === id);
        a.download = artifact?.filename || "download";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(a.href);
      });
  };

  const handlePreview = async (artifact: ApiArtifact) => {
    try {
      const url = artifactsApi.downloadUrl(artifact.id);
      const apiKey = localStorage.getItem("vectoraiz_api_key");
      const resp = await fetch(url, { headers: apiKey ? { "X-API-Key": apiKey } : {} });
      const content = await resp.text();
      setPreview({ artifact, content });
    } catch {}
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="rounded-lg border border-red-500/20 bg-red-500/5 p-4 text-red-400 text-sm">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-foreground">Artifacts</h1>
      </div>

      {artifacts.length === 0 ? (
        <div className="rounded-lg border border-white/10 bg-white/[0.02] p-12 text-center">
          <FileText className="w-12 h-12 text-white/20 mx-auto mb-4" />
          <p className="text-white/50 text-sm">
            No artifacts yet. Ask allAI to create an export, extract, or report from your data.
          </p>
        </div>
      ) : (
        <div className="rounded-lg border border-white/10 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-white/[0.04] text-white/50 text-xs">
                <th className="px-4 py-2.5 text-left font-medium">Filename</th>
                <th className="px-4 py-2.5 text-left font-medium">Format</th>
                <th className="px-4 py-2.5 text-left font-medium">Size</th>
                <th className="px-4 py-2.5 text-left font-medium">Created</th>
                <th className="px-4 py-2.5 text-left font-medium">Source</th>
                <th className="px-4 py-2.5 text-center font-medium w-10">Star</th>
                <th className="px-4 py-2.5 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {artifacts.map((a) => {
                const badge = FORMAT_BADGES[a.format] || FORMAT_BADGES.txt;
                return (
                  <tr key={a.id} className="border-t border-white/5 hover:bg-white/[0.02] transition-colors">
                    <td className="px-4 py-2.5">
                      <button
                        onClick={() => handlePreview(a)}
                        className="text-white/80 hover:text-primary truncate max-w-[300px] block text-left"
                        title={a.description || a.filename}
                      >
                        {a.filename}
                      </button>
                    </td>
                    <td className="px-4 py-2.5">
                      <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-medium ${badge.className}`}>
                        {badge.label}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-white/50 text-xs">{formatSize(a.size_bytes)}</td>
                    <td className="px-4 py-2.5 text-white/50 text-xs">{formatRelativeTime(a.created_at)}</td>
                    <td className="px-4 py-2.5 text-white/50 text-xs">{SOURCE_LABELS[a.source] || a.source}</td>
                    <td className="px-4 py-2.5 text-center">
                      <button
                        onClick={() => handleStar(a.id, !a.starred)}
                        className="hover:scale-110 transition-transform"
                      >
                        <Star
                          className={`w-4 h-4 ${a.starred ? "text-yellow-400 fill-yellow-400" : "text-white/20 hover:text-white/40"}`}
                        />
                      </button>
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center justify-end gap-1">
                        <button
                          onClick={() => handlePreview(a)}
                          className="p-1.5 rounded hover:bg-white/10 text-white/40 hover:text-white/70 transition-colors"
                          title="Preview"
                        >
                          <Eye className="w-3.5 h-3.5" />
                        </button>
                        <button
                          onClick={() => handleDownload(a.id)}
                          className="p-1.5 rounded hover:bg-white/10 text-white/40 hover:text-white/70 transition-colors"
                          title="Download"
                        >
                          <Download className="w-3.5 h-3.5" />
                        </button>
                        <button
                          onClick={() => handleDelete(a.id)}
                          className="p-1.5 rounded hover:bg-red-500/10 text-white/40 hover:text-red-400 transition-colors"
                          title="Delete"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Preview Panel */}
      {preview && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
          <div className="bg-card border border-white/10 rounded-lg w-[90vw] max-w-4xl max-h-[80vh] flex flex-col">
            <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
              <div className="flex items-center gap-3 min-w-0">
                <span className="text-sm font-medium text-white/80 truncate">{preview.artifact.filename}</span>
                <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${FORMAT_BADGES[preview.artifact.format]?.className || ""}`}>
                  {preview.artifact.format.toUpperCase()}
                </span>
                <span className="text-xs text-white/40">{formatSize(preview.artifact.size_bytes)}</span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => handleDownload(preview.artifact.id)}
                  className="p-1.5 rounded hover:bg-white/10 text-white/50 hover:text-white/80"
                >
                  <Download className="w-4 h-4" />
                </button>
                <button
                  onClick={() => setPreview(null)}
                  className="p-1.5 rounded hover:bg-white/10 text-white/50 hover:text-white/80"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-auto p-4">
              <PreviewContent format={preview.artifact.format} content={preview.content} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function PreviewContent({ format, content }: { format: string; content: string }) {
  if (format === "csv") {
    return <CsvPreview content={content} />;
  }
  if (format === "json") {
    return (
      <pre className="text-xs text-white/70 font-mono whitespace-pre-wrap">
        {(() => {
          try {
            return JSON.stringify(JSON.parse(content), null, 2);
          } catch {
            return content;
          }
        })()}
      </pre>
    );
  }
  if (format === "html") {
    return (
      <iframe
        sandbox=""
        srcDoc={content}
        className="w-full h-[60vh] bg-white rounded"
        title="HTML Preview"
      />
    );
  }
  // txt, md — plain text
  return (
    <pre className="text-xs text-white/70 whitespace-pre-wrap">{content}</pre>
  );
}

function CsvPreview({ content }: { content: string }) {
  const lines = content.split("\n").filter((l) => l.trim());
  if (lines.length === 0) return <div className="text-white/40 text-xs italic">Empty CSV</div>;

  const parseRow = (line: string) => {
    const result: string[] = [];
    let current = "";
    let inQuotes = false;
    for (const ch of line) {
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === "," && !inQuotes) {
        result.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
    result.push(current);
    return result;
  };

  const headers = parseRow(lines[0]);
  const rows = lines.slice(1, 101).map(parseRow);

  return (
    <div className="overflow-x-auto rounded border border-white/10">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="bg-white/[0.04]">
            {headers.map((h, i) => (
              <th key={i} className="px-2 py-1.5 text-left font-medium text-white/50 border-b border-white/10 whitespace-nowrap">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={i % 2 === 0 ? "" : "bg-white/[0.02]"}>
              {row.map((cell, j) => (
                <td key={j} className="px-2 py-1 text-white/70 border-b border-white/5 whitespace-nowrap max-w-[200px] truncate">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {lines.length > 101 && (
        <div className="px-3 py-1 text-[10px] text-white/25 bg-white/[0.03]">
          Showing first 100 rows of {lines.length - 1}
        </div>
      )}
    </div>
  );
}
