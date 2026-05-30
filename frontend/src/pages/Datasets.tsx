import React, { useState, useMemo, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Database,
  Upload,
  Search,
  LayoutGrid,
  List,
  FileSpreadsheet,
  FileJson,
  FileText,
  Loader2,
  ChevronDown,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  AlertCircle,
  RefreshCw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import { useBrand } from "@/contexts/BrandContext";
import { useUpload } from "@/contexts/UploadContext";
import DatasetsSkeleton from "@/components/skeletons/DatasetsSkeleton";
import { type Dataset } from "@/types/mockDatasets";
import { useMarketplace } from "@/contexts/MarketplaceContext";
import { useMode } from "@/contexts/ModeContext";
import { cn } from "@/lib/utils";
import { useDatasets } from "@/hooks/useApi";
import { Alert, AlertDescription } from "@/components/ui/alert";
import type { ApiDataset } from "@/lib/api";

type SortField = "name" | "date" | "size" | "rows";
type SortDirection = "asc" | "desc";

const getFileIcon = (type: string) => {
  switch (type.toLowerCase()) {
    case "csv":
    case "tsv":
    case "xlsx":
    case "xls":
      return FileSpreadsheet;
    case "json":
      return FileJson;
    case "pdf":
    case "docx":
    case "doc":
      return FileText;
    case "parquet":
      return Database;
    default:
      return FileText;
  }
};

const formatNumber = (num: number): string => {
  return num.toLocaleString();
};

const formatDate = (date: Date | string): string => {
  const d = typeof date === 'string' ? new Date(date) : date;
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
};

const formatBytes = (bytes?: number): string => {
  if (!bytes) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

// Convert API dataset to local format for display
const convertApiDataset = (apiDataset: ApiDataset): Dataset => ({
  id: apiDataset.id,
  name: apiDataset.original_filename,
  type: apiDataset.file_type as Dataset["type"],
  rows: apiDataset.metadata?.row_count || 0,
  columns: apiDataset.metadata?.column_count || 0,
  size: formatBytes(apiDataset.metadata?.size_bytes),
  sizeBytes: apiDataset.metadata?.size_bytes || 0,
  status: apiDataset.status === 'ready' ? 'ready' : 'processing',
  modifiedAt: new Date(apiDataset.updated_at),
  createdAt: new Date(apiDataset.created_at),
  processingTime: 0,
});

const Datasets = () => {
  const brand = useBrand();
  const navigate = useNavigate();
  const { isPublished, getPublishedData } = useMarketplace();
  const { hasFeature } = useMode();
  const showMarketplace = hasFeature("marketplace");
  const { openModal, setOnSuccess, queue } = useUpload();
  const [searchQuery, setSearchQuery] = useState("");
  const [sortField, setSortField] = useState<SortField>("date");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [viewMode, setViewMode] = useState<"grid" | "list">("grid");
  const { toast } = useToast();

  // Use real API data
  const { data: apiData, loading, error, refetch } = useDatasets();

  // Convert API datasets to local format
  const datasets: Dataset[] = useMemo(() => {
    if (apiData?.datasets) {
      return apiData.datasets.map(convertApiDataset);
    }
    return [];
  }, [apiData]);

  // Register refetch as the upload success callback while this page is mounted
  useEffect(() => {
    setOnSuccess(() => {
      toast({
        title: "Dataset uploaded successfully",
        description: "Your file has been processed and is ready to use.",
      });
      refetch();
    });
  }, [setOnSuccess, refetch]);

  // Refetch dataset list whenever an upload enters "processing" (file accepted by backend)
  const uploadProcessingCount = queue.filter((f) => f.state === "processing").length;
  const prevProcessingCount = React.useRef(0);
  useEffect(() => {
    if (uploadProcessingCount > prevProcessingCount.current) {
      refetch();
    }
    prevProcessingCount.current = uploadProcessingCount;
  }, [uploadProcessingCount, refetch]);

  // Poll dataset list while any dataset has "processing" status
  const hasProcessingDatasets = datasets.some((d) => d.status === "processing");
  useEffect(() => {
    if (!hasProcessingDatasets) return;
    const interval = setInterval(() => refetch(), 5000);
    return () => clearInterval(interval);
  }, [hasProcessingDatasets, refetch]);

  const handleDatasetClick = (dataset: Dataset) => {
    navigate(`/datasets/${dataset.id}`);
  };

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
  };

  const filteredAndSortedDatasets = useMemo(() => {
    let filtered = datasets.filter((dataset) =>
      dataset.name.toLowerCase().includes(searchQuery.toLowerCase())
    );

    filtered.sort((a, b) => {
      let comparison = 0;
      switch (sortField) {
        case "name":
          comparison = a.name.localeCompare(b.name);
          break;
        case "date":
          comparison = a.modifiedAt.getTime() - b.modifiedAt.getTime();
          break;
        case "size":
          comparison = a.sizeBytes - b.sizeBytes;
          break;
        case "rows":
          comparison = a.rows - b.rows;
          break;
      }
      return sortDirection === "asc" ? comparison : -comparison;
    });

    return filtered;
  }, [datasets, searchQuery, sortField, sortDirection]);

  const sortLabels: Record<SortField, string> = {
    name: "Name",
    date: "Date",
    size: "Size",
    rows: "Rows",
  };

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) return ArrowUpDown;
    return sortDirection === "asc" ? ArrowUp : ArrowDown;
  };

  const isEmpty = datasets.length === 0;

  if (loading) {
    return <DatasetsSkeleton />;
  }

  return (
    <div className={cn("space-y-6 animate-in fade-in duration-300")}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <p className="text-muted-foreground">
          Manage and browse your processed datasets
        </p>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => refetch()} className="gap-2">
            <RefreshCw className="w-4 h-4" />
            Refresh
          </Button>
          <Button className="gap-2" onClick={() => openModal()}>
            <Upload className="w-4 h-4" />
            Upload
          </Button>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>
            Failed to load datasets from backend: {error}. Showing local data.
            <Button variant="link" className="p-0 ml-2 h-auto" onClick={() => refetch()}>
              Retry
            </Button>
          </AlertDescription>
        </Alert>
      )}

      {isEmpty ? (
        /* Empty State */
        <Card className="bg-card border-border border-dashed">
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                <Database className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">
                  No datasets yet
                </h3>
                <p className="text-muted-foreground max-w-sm">
                  Upload your first file to get started. {brand.name} supports CSV,
                  JSON, Parquet, and more.
                </p>
              </div>
              <Button
                variant="secondary"
                className="gap-2 mt-2"
                onClick={() => openModal()}
              >
                <Upload className="w-4 h-4" />
                Upload your first file
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Filter/Sort Bar */}
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
            <div className="relative w-full sm:w-72">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                placeholder="Search datasets..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9 bg-secondary border-border"
              />
            </div>

            <div className="flex items-center gap-2">
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="sm" className="gap-2">
                    Sort: {sortLabels[sortField]}
                    <ChevronDown className="w-4 h-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={() => handleSort("name")}>
                    Name
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => handleSort("date")}>
                    Date
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => handleSort("size")}>
                    Size
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => handleSort("rows")}>
                    Rows
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>

              <div className="flex items-center border border-border rounded-md">
                <Button
                  variant={viewMode === "grid" ? "secondary" : "ghost"}
                  size="sm"
                  className="rounded-r-none"
                  onClick={() => setViewMode("grid")}
                >
                  <LayoutGrid className="w-4 h-4" />
                </Button>
                <Button
                  variant={viewMode === "list" ? "secondary" : "ghost"}
                  size="sm"
                  className="rounded-l-none"
                  onClick={() => setViewMode("list")}
                >
                  <List className="w-4 h-4" />
                </Button>
              </div>
            </div>
          </div>

          {/* Grid View */}
          {viewMode === "grid" && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {filteredAndSortedDatasets.map((dataset) => {
                const Icon = getFileIcon(dataset.type);
                const datasetPublished = showMarketplace && (isPublished(dataset.id) || dataset.marketplace?.isPublished);
                const publishedData = getPublishedData(dataset.id) || dataset.marketplace;
                return (
                  <Card
                    key={dataset.id}
                    className="bg-card border-border card-hover cursor-pointer"
                    onClick={() => handleDatasetClick(dataset)}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start gap-3">
                        <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center flex-shrink-0">
                          <Icon className="w-5 h-5 text-primary" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <h3 className="font-medium text-foreground truncate">
                            {dataset.name}
                          </h3>
                          <p className="text-sm text-muted-foreground mt-1">
                            {formatNumber(dataset.rows)} rows •{" "}
                            {dataset.columns} columns
                          </p>
                        </div>
                      </div>

                      <div className="mt-4 flex items-center justify-between">
                        <span className="text-sm text-muted-foreground">
                          {dataset.size}
                        </span>
                        <div className="flex items-center gap-2">
                          {datasetPublished && (
                            <Badge
                              variant="secondary"
                              className="bg-[hsl(var(--haven-success))]/20 text-[hsl(var(--haven-success))] border-[hsl(var(--haven-success))]/30"
                            >
                              Published
                            </Badge>
                          )}
                          {dataset.status === "ready" ? (
                            <Badge
                              variant="secondary"
                              className="bg-haven-success/20 text-haven-success border-haven-success/30"
                            >
                              Ready
                            </Badge>
                          ) : (
                            <Badge
                              variant="secondary"
                              className="bg-haven-warning/20 text-haven-warning border-haven-warning/30 gap-1"
                            >
                              <Loader2 className="w-3 h-3 animate-spin" />
                              Processing
                            </Badge>
                          )}
                        </div>
                      </div>

                      <div className="mt-3 flex items-center justify-between">
                        <p className="text-xs text-muted-foreground">
                          Modified {formatDate(dataset.modifiedAt)}
                        </p>
                        {datasetPublished && publishedData && (
                          <span className="text-sm font-medium text-primary">
                            ${publishedData.price}
                          </span>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}

          {/* List View */}
          {viewMode === "list" && (
            <Card className="bg-card border-border overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent border-border">
                    <TableHead
                      className="cursor-pointer"
                      onClick={() => handleSort("name")}
                    >
                      <div className="flex items-center gap-2">
                        Name
                        {React.createElement(getSortIcon("name"), {
                          className: "w-4 h-4",
                        })}
                      </div>
                    </TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead
                      className="cursor-pointer"
                      onClick={() => handleSort("rows")}
                    >
                      <div className="flex items-center gap-2">
                        Rows
                        {React.createElement(getSortIcon("rows"), {
                          className: "w-4 h-4",
                        })}
                      </div>
                    </TableHead>
                    <TableHead>Columns</TableHead>
                    <TableHead
                      className="cursor-pointer"
                      onClick={() => handleSort("size")}
                    >
                      <div className="flex items-center gap-2">
                        Size
                        {React.createElement(getSortIcon("size"), {
                          className: "w-4 h-4",
                        })}
                      </div>
                    </TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Marketplace</TableHead>
                    <TableHead
                      className="cursor-pointer"
                      onClick={() => handleSort("date")}
                    >
                      <div className="flex items-center gap-2">
                        Modified
                        {React.createElement(getSortIcon("date"), {
                          className: "w-4 h-4",
                        })}
                      </div>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredAndSortedDatasets.map((dataset) => {
                    const Icon = getFileIcon(dataset.type);
                    const datasetPublished = showMarketplace && (isPublished(dataset.id) || dataset.marketplace?.isPublished);
                    const publishedData = getPublishedData(dataset.id) || dataset.marketplace;
                    return (
                      <TableRow
                        key={dataset.id}
                        className="cursor-pointer border-border hover:bg-secondary/50"
                        onClick={() => handleDatasetClick(dataset)}
                      >
                        <TableCell>
                          <div className="flex items-center gap-3">
                            <Icon className="w-4 h-4 text-primary" />
                            <span className="font-medium">{dataset.name}</span>
                          </div>
                        </TableCell>
                        <TableCell className="uppercase text-xs text-muted-foreground">
                          {dataset.type}
                        </TableCell>
                        <TableCell>{formatNumber(dataset.rows)}</TableCell>
                        <TableCell>{dataset.columns}</TableCell>
                        <TableCell>{dataset.size}</TableCell>
                        <TableCell>
                          {dataset.status === "ready" ? (
                            <Badge
                              variant="secondary"
                              className="bg-haven-success/20 text-haven-success border-haven-success/30"
                            >
                              Ready
                            </Badge>
                          ) : (
                            <Badge
                              variant="secondary"
                              className="bg-haven-warning/20 text-haven-warning border-haven-warning/30 gap-1"
                            >
                              <Loader2 className="w-3 h-3 animate-spin" />
                              Processing
                            </Badge>
                          )}
                        </TableCell>
                        <TableCell>
                          {datasetPublished ? (
                            <div className="flex items-center gap-2">
                              <Badge
                                variant="secondary"
                                className="bg-[hsl(var(--haven-success))]/20 text-[hsl(var(--haven-success))] border-[hsl(var(--haven-success))]/30"
                              >
                                Published
                              </Badge>
                              {publishedData && (
                                <span className="text-sm font-medium text-primary">
                                  ${publishedData.price}
                                </span>
                              )}
                            </div>
                          ) : (
                            <span className="text-muted-foreground text-sm">—</span>
                          )}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {formatDate(dataset.modifiedAt)}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </Card>
          )}
        </>
      )}

    </div>
  );
};

export default Datasets;
