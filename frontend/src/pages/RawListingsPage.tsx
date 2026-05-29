import { useCallback, useEffect, useState } from "react";
import {
  Upload,
  RefreshCw,
  FileText,
  Store,
  CheckCircle2,
  Loader2,
  AlertCircle,
  Tag,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useUpload } from "@/contexts/UploadContext";
import { useBrand } from "@/contexts/BrandContext";
import { useToast } from "@/hooks/use-toast";
import { rawFilesApi, type RawFile } from "@/lib/api";
import RawPublishModal from "@/components/RawPublishModal";

const formatBytes = (bytes: number): string => {
  if (!bytes) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
};

const formatDate = (d: string): string => {
  const date = new Date(d);
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
};

const listingBadge = (status: string | null) => {
  if (status === "listed") {
    return (
      <Badge className="bg-green-500/10 text-green-600 border-green-500/30">
        <CheckCircle2 className="w-3 h-3 mr-1" />
        Listed
      </Badge>
    );
  }
  if (status === "draft") {
    return (
      <Badge variant="secondary">
        <Tag className="w-3 h-3 mr-1" />
        Draft
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-muted-foreground">
      Not listed
    </Badge>
  );
};

const RawListingsPage = () => {
  const brand = useBrand();
  const { openModal, setOnSuccess } = useUpload();
  const { toast } = useToast();

  const [rawFiles, setRawFiles] = useState<RawFile[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [publishTarget, setPublishTarget] = useState<RawFile | null>(null);

  const fetchRawFiles = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await rawFilesApi.listRawFiles();
      setRawFiles(data);
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to load files";
      setError(msg);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRawFiles();
  }, [fetchRawFiles]);

  useEffect(() => {
    setOnSuccess(() => fetchRawFiles);
  }, [setOnSuccess, fetchRawFiles]);

  const handlePublishSuccess = (file: RawFile) => {
    toast({
      title: "Listed on ai.market",
      description: `${file.filename} is now visible to buyers.`,
    });
    setPublishTarget(null);
    fetchRawFiles();
  };

  const isEmpty = !loading && rawFiles.length === 0;

  return (
    <div className="space-y-6 animate-in fade-in duration-300">
      <div className="flex items-center justify-between">
        <p className="text-muted-foreground">
          Upload files and list them on ai.market — no processing required.
        </p>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => fetchRawFiles()} className="gap-2">
            <RefreshCw className="w-4 h-4" />
            Refresh
          </Button>
          <Button className="gap-2" onClick={() => openModal()}>
            <Upload className="w-4 h-4" />
            Upload file
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>
            Failed to load files: {error}
            <Button variant="link" className="p-0 ml-2 h-auto" onClick={() => fetchRawFiles()}>
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      )}

      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="py-16 flex items-center justify-center">
            <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
          </CardContent>
        </Card>
      )}

      {isEmpty && (
        <Card className="bg-card border-border border-dashed">
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                <Store className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">No files yet</h3>
                <p className="text-muted-foreground max-w-sm">
                  Upload a file to list it on ai.market. {brand.name} accepts any file type —
                  your data, your format.
                </p>
              </div>
              <Button variant="secondary" className="gap-2 mt-2" onClick={() => openModal()}>
                <Upload className="w-4 h-4" />
                Upload your first file
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {!loading && rawFiles.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>File</TableHead>
                  <TableHead className="w-32">Size</TableHead>
                  <TableHead className="w-40">Uploaded</TableHead>
                  <TableHead className="w-32">Status</TableHead>
                  <TableHead className="w-40 text-right">Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rawFiles.map((f) => (
                  <TableRow key={f.id}>
                    <TableCell>
                      <div className="flex items-center gap-3">
                        <FileText className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                        <span className="font-medium text-foreground truncate">{f.filename}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-muted-foreground">{formatBytes(f.file_size_bytes)}</TableCell>
                    <TableCell className="text-muted-foreground">{formatDate(f.created_at)}</TableCell>
                    <TableCell>{listingBadge(f.listing_status)}</TableCell>
                    <TableCell className="text-right">
                      {f.listing_status === "listed" ? (
                        <Button size="sm" variant="ghost" disabled>
                          On ai.market
                        </Button>
                      ) : (
                        <Button size="sm" onClick={() => setPublishTarget(f)} className="gap-2">
                          <Store className="w-3.5 h-3.5" />
                          Publish to ai.market
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {publishTarget && (
        <RawPublishModal
          open={!!publishTarget}
          onOpenChange={(open) => !open && setPublishTarget(null)}
          rawFile={publishTarget}
          onPublishSuccess={() => handlePublishSuccess(publishTarget)}
        />
      )}
    </div>
  );
};

export default RawListingsPage;
