import { useState, useEffect } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import {
  ArrowLeft,
  Search,
  Code,
  Trash2,
  FileSpreadsheet,
  FileJson,
  FileText,
  Database,
  Loader2,
  Rows3,
  Columns3,
  HardDrive,
  Calendar,
  Clock,
  FileType,
  ChevronLeft,
  ChevronRight,
  Upload,
  ExternalLink,
  Eye,
  ShoppingCart,
  DollarSign,
  TrendingUp,
  ChevronRight as ChevronRightIcon,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  datasetsApi,
  type ApiDataset,
  type DatasetSampleResponse,
  type DatasetStatisticsResponse,
} from "@/lib/api";
import { type ColumnSchema, type Dataset } from "@/data/mockDatasets";
import { toast } from "@/hooks/use-toast";
import PublishModal from "@/components/PublishModal";
import DatasetPreview from "@/components/DatasetPreview";
import { useMarketplace } from "@/contexts/MarketplaceContext";
import { useMode } from "@/contexts/ModeContext";

const getFileIcon = (type: Dataset["type"]) => {
  switch (type) {
    case "csv":
    case "xlsx":
      return FileSpreadsheet;
    case "json":
      return FileJson;
    case "pdf":
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

const formatDate = (date: Date): string => {
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
};

const DataTypeColors: Record<ColumnSchema["dataType"], string> = {
  string: "bg-blue-500/20 text-blue-400 border-blue-500/30",
  integer: "bg-green-500/20 text-green-400 border-green-500/30",
  float: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
  date: "bg-purple-500/20 text-purple-400 border-purple-500/30",
  boolean: "bg-pink-500/20 text-pink-400 border-pink-500/30",
};

// Map API column type to schema data type
const mapApiTypeToSchemaType = (apiType: string | undefined | null): ColumnSchema["dataType"] => {
  if (!apiType) return "string";
  
  const type = apiType.toLowerCase();
  if (type.includes("int") || type.includes("bigint")) return "integer";
  if (type.includes("float") || type.includes("double") || type.includes("decimal") || type.includes("number")) return "float";
  if (type.includes("date") || type.includes("time")) return "date";
  if (type.includes("bool")) return "boolean";
  return "string";
};

// Helper to convert API dataset to frontend format
const mapApiDatasetToFrontend = (apiDataset: ApiDataset): Dataset => ({
  id: apiDataset.id,
  name: apiDataset.original_filename,
  type: apiDataset.file_type as "csv" | "xlsx" | "json" | "pdf" | "parquet",
  status: apiDataset.status === "ready" ? "ready" as const
    : apiDataset.status === "error" ? "error" as const
    : apiDataset.status === "preview_ready" ? "preview_ready" as const
    : apiDataset.status === "cancelled" ? "error" as const
    : "processing" as const,
  rows: apiDataset.metadata?.row_count || 0,
  columns: apiDataset.metadata?.column_count || 0,
  size: apiDataset.metadata?.size_bytes
    ? `${(apiDataset.metadata.size_bytes / 1024 / 1024).toFixed(2)} MB`
    : "Unknown",
  sizeBytes: apiDataset.metadata?.size_bytes || 0,
  createdAt: new Date(apiDataset.created_at),
  modifiedAt: new Date(apiDataset.updated_at),
  processingTime: 0,
  marketplace: undefined,
});

const DatasetDetail = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { isPublished, getPublishedData, unpublishDataset } = useMarketplace();
  const { hasFeature } = useMode();
  const [currentPage, setCurrentPage] = useState(1);
  const [publishModalOpen, setPublishModalOpen] = useState(false);
  const rowsPerPage = 10;

  // Delete debounce
  const [isDeleting, setIsDeleting] = useState(false);

  // API data states
  const [apiDataset, setApiDataset] = useState<ApiDataset | null>(null);
  const [sampleData, setSampleData] = useState<Record<string, unknown>[]>([]);
  const [statistics, setStatistics] = useState<DatasetStatisticsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch dataset from API
  useEffect(() => {
    if (!id) {
      setError("No dataset ID provided");
      setLoading(false);
      return;
    }

    const fetchDataset = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await datasetsApi.get(id);
        setApiDataset(data);

        // Fetch sample data and statistics in parallel
        const [sampleRes, statsRes] = await Promise.allSettled([
          datasetsApi.getSample(id, 20),
          datasetsApi.getStatistics(id),
        ]);

        if (sampleRes.status === "fulfilled") {
          setSampleData(sampleRes.value.sample);
        }
        if (statsRes.status === "fulfilled") {
          setStatistics(statsRes.value);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load dataset");
      } finally {
        setLoading(false);
      }
    };

    fetchDataset();
  }, [id]);

  // Show loading skeleton
  if (loading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-4 w-48" />
        <div className="flex items-center gap-4">
          <Skeleton className="w-12 h-12 rounded-lg" />
          <div className="space-y-2">
            <Skeleton className="h-6 w-64" />
            <Skeleton className="h-4 w-40" />
          </div>
        </div>
        <div className="grid grid-cols-6 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-24 rounded-lg" />
          ))}
        </div>
      </div>
    );
  }

  // Show error / not found
  if (error || !apiDataset) {
    return (
      <div className="flex flex-col items-center justify-center py-16 space-y-4">
        <Database className="w-16 h-16 text-muted-foreground" />
        <h2 className="text-xl font-semibold text-foreground">
          Dataset not found
        </h2>
        <p className="text-muted-foreground">
          {error || "The dataset you're looking for doesn't exist."}
        </p>
        <Button variant="secondary" onClick={() => navigate("/datasets")}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Datasets
        </Button>
      </div>
    );
  }

  // Convert API dataset to frontend format
  const dataset = mapApiDatasetToFrontend(apiDataset);
  const Icon = getFileIcon(dataset.type);

  // Build schema from API data
  const schema: ColumnSchema[] = apiDataset.metadata?.columns?.map((col) => ({
    name: col.name,
    dataType: mapApiTypeToSchemaType(col.type),
    nonNullCount: dataset.rows,
    nullPercentage: 0,
    sampleValues: [],
  })) || [];

  // Build stats from API response
  const stats = statistics?.statistics?.map((s) => ({
    name: s.column,
    dataType: mapApiTypeToSchemaType(s.type),
    uniqueCount: s.unique_count,
    min: s.min,
    max: s.max,
    mean: s.mean,
    median: s.median,
    stdDev: s.std,
    mostCommon: s.top_values?.map((v) => ({ value: v.value, count: v.count })),
  })) || [];

  const totalPages = Math.ceil(dataset.rows / rowsPerPage);
  const startRow = (currentPage - 1) * rowsPerPage + 1;
  const endRow = Math.min(currentPage * rowsPerPage, dataset.rows);

  // Check marketplace context for published status
  const datasetIsPublished = isPublished(dataset.id) || dataset.marketplace?.isPublished;
  const publishedData = getPublishedData(dataset.id);
  const marketplaceData = publishedData || dataset.marketplace;

  const handlePublishSuccess = () => {
    toast({
      title: "Dataset published",
      description: "Your dataset is now live on the marketplace",
    });
  };

  const handleUnpublish = () => {
    unpublishDataset(dataset.id);
    toast({
      title: "Dataset unpublished",
      description: "Your dataset has been removed from the marketplace",
    });
  };

  const handleDelete = async () => {
    if (!window.confirm("Are you sure you want to delete this dataset? This action cannot be undone.")) {
      return;
    }
    setIsDeleting(true);
    try {
      await datasetsApi.delete(dataset.id);
      toast({
        title: "Dataset deleted",
        description: "The dataset has been permanently removed",
      });
      navigate("/datasets");
    } catch (e) {
      toast({
        title: "Delete failed",
        description: "Failed to delete dataset. Please try again or check system health.",
        variant: "destructive",
      });
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-1 text-sm">
        <Link
          to="/datasets"
          className="text-muted-foreground hover:text-foreground transition-colors"
        >
          Datasets
        </Link>
        <ChevronRightIcon className="w-4 h-4 text-muted-foreground" />
        <span className="text-foreground font-medium">{dataset.name}</span>
      </nav>

      {/* Header */}
      <div className="flex flex-col gap-4">
        <Button
          variant="ghost"
          size="sm"
          className="w-fit -ml-2 text-muted-foreground hover:text-foreground"
          onClick={() => navigate("/datasets")}
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Datasets
        </Button>

        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-lg bg-secondary flex items-center justify-center">
              <Icon className="w-6 h-6 text-primary" />
            </div>
            <div>
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-bold text-foreground">
                  {dataset.name}
                </h1>
                {datasetIsPublished && (
                  <Badge className="bg-[hsl(var(--haven-success))]/20 text-[hsl(var(--haven-success))] border-[hsl(var(--haven-success))]/30">
                    Published
                  </Badge>
                )}
              </div>
              <div className="flex items-center gap-3 mt-1">
                {dataset.status === "ready" ? (
                  <Badge
                    variant="secondary"
                    className="bg-haven-success/20 text-haven-success border-haven-success/30"
                  >
                    Ready
                  </Badge>
                ) : dataset.status === "preview_ready" ? (
                  <Badge
                    variant="secondary"
                    className="bg-primary/20 text-primary border-primary/30"
                  >
                    Preview Ready
                  </Badge>
                ) : dataset.status === "error" ? (
                  <Badge
                    variant="secondary"
                    className="bg-destructive/20 text-destructive border-destructive/30"
                  >
                    Failed
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
                <span className="text-sm text-muted-foreground">
                  {formatNumber(dataset.rows)} rows • {dataset.columns} columns
                </span>
                {hasFeature("marketplace") && datasetIsPublished && marketplaceData && (
                  <span className="text-sm font-medium text-primary">
                    ${marketplaceData.price}
                  </span>
                )}
              </div>
            </div>
          </div>

          {dataset.status !== "preview_ready" && (
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => navigate(`/search?dataset=${dataset.id}`)}
            >
              <Search className="w-4 h-4 mr-2" />
              Search
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => navigate(`/sql?dataset=${dataset.id}`)}
            >
              <Code className="w-4 h-4 mr-2" />
              Query
            </Button>
            {hasFeature("marketplace") && dataset.status === "ready" && !datasetIsPublished && (
              <Button
                variant="default"
                size="sm"
                onClick={() => setPublishModalOpen(true)}
                className="gap-2"
              >
                <Upload className="w-4 h-4" />
                Publish
              </Button>
            )}
            <Button variant="outline" size="sm" className="text-destructive hover:text-destructive" onClick={handleDelete} disabled={isDeleting}>
              <Trash2 className="w-4 h-4 mr-2" />
              Delete
            </Button>
          </div>
          )}
        </div>
      </div>

      {/* Error/failed status message */}
      {dataset.status === "error" && (
        <Card className="border-destructive/50 bg-destructive/5">
          <CardContent className="py-6">
            <div className="flex items-center gap-3">
              <Database className="w-8 h-8 text-destructive" />
              <div>
                <h3 className="text-base font-semibold text-foreground">Processing Failed</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  {apiDataset.error || "This dataset failed during processing. You can delete it and re-upload."}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Preview UI for preview_ready status */}
      {dataset.status === "preview_ready" && (
        <DatasetPreview datasetId={dataset.id} />
      )}

      {/* Tabs — only shown when dataset is fully ready */}
      {dataset.status !== "preview_ready" && (
      <Tabs defaultValue="overview" className="space-y-6">
        <TabsList className="bg-secondary">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="schema">Schema</TabsTrigger>
          <TabsTrigger value="sample">Sample Data</TabsTrigger>
          <TabsTrigger value="statistics">Statistics</TabsTrigger>
          {hasFeature("marketplace") && datasetIsPublished && (
            <TabsTrigger value="marketplace">Marketplace</TabsTrigger>
          )}
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <Rows3 className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Total Rows</p>
                    <p className="text-lg font-semibold text-foreground">
                      {formatNumber(dataset.rows)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <Columns3 className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">
                      Total Columns
                    </p>
                    <p className="text-lg font-semibold text-foreground">
                      {dataset.columns}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <HardDrive className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">File Size</p>
                    <p className="text-lg font-semibold text-foreground">
                      {dataset.size}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <Calendar className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Created</p>
                    <p className="text-lg font-semibold text-foreground">
                      {formatDate(dataset.createdAt)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <Clock className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Modified</p>
                    <p className="text-lg font-semibold text-foreground">
                      {formatDate(dataset.modifiedAt)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center">
                    <FileType className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">File Type</p>
                    <p className="text-lg font-semibold text-foreground uppercase">
                      {dataset.type}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {dataset.status === "ready" && (
            <Card className="bg-card border-border">
              <CardContent className="py-4">
                <p className="text-sm text-muted-foreground">
                  <Clock className="w-4 h-4 inline mr-2" />
                  Processed in{" "}
                  <span className="text-foreground font-medium">
                    {dataset.processingTime} seconds
                  </span>
                </p>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* Schema Tab */}
        <TabsContent value="schema">
          <Card className="bg-card border-border overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent border-border">
                  <TableHead>Column Name</TableHead>
                  <TableHead>Data Type</TableHead>
                  <TableHead>Non-Null Count</TableHead>
                  <TableHead>Null %</TableHead>
                  <TableHead>Sample Values</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {schema.map((col) => (
                  <TableRow
                    key={col.name}
                    className="border-border hover:bg-secondary/50"
                  >
                    <TableCell className="font-mono text-sm">
                      {col.name}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant="outline"
                        className={DataTypeColors[col.dataType]}
                      >
                        {col.dataType}
                      </Badge>
                    </TableCell>
                    <TableCell>{formatNumber(col.nonNullCount)}</TableCell>
                    <TableCell>
                      <span
                        className={
                          col.nullPercentage > 0
                            ? "text-haven-warning"
                            : "text-haven-success"
                        }
                      >
                        {col.nullPercentage}%
                      </span>
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {col.sampleValues.join(", ")}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
        </TabsContent>

        {/* Sample Data Tab */}
        <TabsContent value="sample" className="space-y-4">
          <Card className="bg-card border-border overflow-hidden">
            <ScrollArea className="w-full">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent border-border">
                    {(sampleData[0] ? Object.keys(sampleData[0]) : []).map((key) => (
                      <TableHead key={key} className="whitespace-nowrap">
                        {key}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sampleData.map((row, index) => (
                    <TableRow
                      key={index}
                      className="border-border hover:bg-secondary/50"
                    >
                      {Object.values(row).map((value, cellIndex) => (
                        <TableCell
                          key={cellIndex}
                          className="whitespace-nowrap"
                        >
                          {value === null ? (
                            <span className="text-muted-foreground italic">
                              null
                            </span>
                          ) : (
                            String(value)
                          )}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          </Card>

          {/* Pagination */}
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Showing rows {formatNumber(startRow)}-{formatNumber(endRow)} of{" "}
              {formatNumber(dataset.rows)}
            </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage === 1}
                onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
              >
                <ChevronLeft className="w-4 h-4 mr-1" />
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage >= totalPages}
                onClick={() =>
                  setCurrentPage((p) => Math.min(totalPages, p + 1))
                }
              >
                Next
                <ChevronRight className="w-4 h-4 ml-1" />
              </Button>
            </div>
          </div>
        </TabsContent>

        {/* Statistics Tab */}
        <TabsContent value="statistics" className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {stats.map((stat) => (
              <Card key={stat.name} className="bg-card border-border">
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base font-mono">
                      {stat.name}
                    </CardTitle>
                    <Badge
                      variant="outline"
                      className={DataTypeColors[stat.dataType]}
                    >
                      {stat.dataType}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  {stat.dataType === "integer" || stat.dataType === "float" ? (
                    <div className="grid grid-cols-5 gap-4 text-center">
                      <div>
                        <p className="text-xs text-muted-foreground">Min</p>
                        <p className="text-sm font-semibold text-foreground">
                          {stat.min?.toLocaleString()}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Max</p>
                        <p className="text-sm font-semibold text-foreground">
                          {stat.max?.toLocaleString()}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Mean</p>
                        <p className="text-sm font-semibold text-foreground">
                          {stat.mean?.toLocaleString(undefined, {
                            maximumFractionDigits: 2,
                          })}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Median</p>
                        <p className="text-sm font-semibold text-foreground">
                          {stat.median?.toLocaleString()}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Std Dev</p>
                        <p className="text-sm font-semibold text-foreground">
                          {stat.stdDev?.toLocaleString(undefined, {
                            maximumFractionDigits: 2,
                          })}
                        </p>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <p className="text-sm">
                        <span className="text-muted-foreground">
                          Unique values:{" "}
                        </span>
                        <span className="font-semibold text-foreground">
                          {stat.uniqueCount?.toLocaleString()}
                        </span>
                      </p>
                      {stat.mostCommon && (
                        <div>
                          <p className="text-xs text-muted-foreground mb-2">
                            Most common values:
                          </p>
                          <div className="space-y-1">
                            {stat.mostCommon.map((item, i) => (
                              <div
                                key={i}
                                className="flex items-center justify-between text-sm"
                              >
                                <span className="text-foreground truncate max-w-[200px]">
                                  {item.value}
                                </span>
                                <span className="text-muted-foreground">
                                  {item.count.toLocaleString()}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>

        {/* Marketplace Tab */}
        {datasetIsPublished && (
          <TabsContent value="marketplace" className="space-y-6">
            {/* Listing Status */}
            <Card className="bg-card border-border">
              <CardContent className="py-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 rounded-full bg-[hsl(var(--haven-success))]/20 flex items-center justify-center">
                      <TrendingUp className="w-6 h-6 text-[hsl(var(--haven-success))]" />
                    </div>
                    <div>
                      <h3 className="text-lg font-semibold text-foreground">Live on Marketplace</h3>
                      <p className="text-sm text-muted-foreground">
                        Listed at ${marketplaceData?.price || 450}
                      </p>
                    </div>
                  </div>
                  <Button variant="outline" className="gap-2">
                    <ExternalLink className="w-4 h-4" />
                    View Listing
                  </Button>
                </div>
              </CardContent>
            </Card>

            {/* Stats Grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="bg-card border-border">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 rounded-lg bg-secondary flex items-center justify-center">
                      <Eye className="w-6 h-6 text-primary" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Views</p>
                      <p className="text-2xl font-bold text-foreground">
                        {marketplaceData?.views || 145}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-card border-border">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 rounded-lg bg-secondary flex items-center justify-center">
                      <ShoppingCart className="w-6 h-6 text-primary" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Purchases</p>
                      <p className="text-2xl font-bold text-foreground">
                        {marketplaceData?.purchases || 3}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-card border-border">
                <CardContent className="p-6">
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 rounded-lg bg-[hsl(var(--haven-success))]/20 flex items-center justify-center">
                      <DollarSign className="w-6 h-6 text-[hsl(var(--haven-success))]" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Earnings</p>
                      <p className="text-2xl font-bold text-[hsl(var(--haven-success))]">
                        ${(marketplaceData?.earnings || 1080).toLocaleString()}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Actions */}
            <div className="flex gap-3">
              <Button variant="outline" className="gap-2">
                Update Listing
              </Button>
              <Button 
                variant="outline" 
                className="gap-2 text-destructive hover:text-destructive"
                onClick={handleUnpublish}
              >
                Unpublish
              </Button>
            </div>
          </TabsContent>
        )}
      </Tabs>
      )}

      {/* Publish Modal */}
      <PublishModal
        open={publishModalOpen}
        onOpenChange={setPublishModalOpen}
        dataset={dataset}
        onPublishSuccess={handlePublishSuccess}
      />
    </div>
  );
};

export default DatasetDetail;