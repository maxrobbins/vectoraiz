import { useState, useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import {
  Search,
  Database,
  ArrowLeft,
  Sparkles,
  Loader2,
  ExternalLink,
  Copy,
  AlertCircle,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { toast } from "sonner";
import { useSearch, useDatasets } from "@/hooks/useApi";
import type { SearchResult } from "@/lib/api";

const exampleQueries = [
  "Show me high value orders over $1000",
  "Find customers from California",
  "Recent orders with shipping delays",
];

const SearchPage = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const datasetParam = searchParams.get("dataset");

  const [selectedDatasetId, setSelectedDatasetId] = useState<string>("__all__");
  const [searchQuery, setSearchQuery] = useState("");
  const [hasSearched, setHasSearched] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<SearchResult | null>(null);

  // API hooks
  const { data: datasetsData, loading: datasetsLoading, error: datasetsError } = useDatasets();
  const { search, results, loading: isSearching, error } = useSearch();

  // Get datasets list from API only
  const datasets = datasetsData?.datasets
    ?.filter(d => d.status === 'ready')
    ?.map(d => ({ id: d.id, name: d.original_filename, status: d.status })) || [];

  // Auto-select dataset from URL parameter
  useEffect(() => {
    if (datasetParam && datasets.length > 0) {
      const dataset = datasets.find(d => d.id === datasetParam);
      if (dataset) {
        setSelectedDatasetId(datasetParam);
      }
    }
  }, [datasetParam, datasets]);

  const isAllDatasets = selectedDatasetId === "__all__";
  const selectedDataset = isAllDatasets
    ? { id: "__all__", name: "All datasets", status: "ready" }
    : selectedDatasetId
      ? datasets.find(d => d.id === selectedDatasetId)
      : null;

  const handleExampleClick = (query: string) => {
    setSearchQuery(query);
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) {
      toast.error("Please enter a search query");
      return;
    }

    if (!selectedDataset) return;

    setHasSearched(true);
    await search(searchQuery, isAllDatasets ? {} : { dataset_id: selectedDatasetId });
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && (selectedDataset || isAllDatasets)) {
      handleSearch();
    }
  };

  const handleCopyJson = (data: Record<string, unknown>) => {
    navigator.clipboard.writeText(JSON.stringify(data, null, 2));
    toast.success("Copied to clipboard as JSON");
  };

  const handleOpenInSqlQuery = () => {
    const datasetId = selectedRecord?.dataset_id ?? selectedDatasetId;
    setSelectedRecord(null);
    navigate(`/sql?dataset=${datasetId}`);
  };

  const getRelevanceColor = (score: number) => {
    const percentage = score * 100;
    if (percentage >= 90) return "bg-haven-success";
    if (percentage >= 75) return "bg-primary";
    return "bg-haven-warning";
  };

  return (
    <div className="space-y-6">
      {/* Back button if accessed from dataset */}
      {datasetParam && selectedDataset && (
        <Button
          variant="ghost"
          size="sm"
          className="w-fit -ml-2 text-muted-foreground hover:text-foreground"
          onClick={() => navigate(`/datasets/${datasetParam}`)}
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to {selectedDataset.name}
        </Button>
      )}

      <div>
        <p className="text-muted-foreground">
          Perform semantic search across your datasets
        </p>
      </div>

      {/* Dataset Selector */}
      <div className="max-w-md">
        <label className="text-sm font-medium text-foreground mb-2 block">
          Select Dataset
        </label>
        {datasetsLoading ? (
          <Skeleton className="h-10 w-full" />
        ) : datasetsError ? (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>Failed to load datasets: {datasetsError}</AlertDescription>
          </Alert>
        ) : datasets.length === 0 ? (
          <div className="text-sm text-muted-foreground p-3 border border-dashed rounded-lg">
            No datasets available. Upload a dataset first.
          </div>
        ) : (
          <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
            <SelectTrigger className="bg-secondary border-border">
              <SelectValue placeholder="Choose a dataset..." />
            </SelectTrigger>
            <SelectContent className="bg-card border-border z-50">
              <SelectItem value="__all__">All datasets</SelectItem>
              {datasets.map((dataset) => (
                <SelectItem key={dataset.id} value={dataset.id}>
                  {dataset.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      </div>

      {/* Search Input */}
      <div className="flex gap-2 max-w-2xl">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Search your data using natural language..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            className="pl-10 h-12 bg-secondary border-border text-foreground placeholder:text-muted-foreground"
            disabled={!selectedDataset && !isAllDatasets}
          />
        </div>
        <Button
          className="h-12 px-6"
          disabled={(!selectedDataset && !isAllDatasets) || isSearching}
          onClick={handleSearch}
        >
          {isSearching ? (
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
          ) : (
            <Search className="w-4 h-4 mr-2" />
          )}
          Search
        </Button>
      </div>

      {/* Example Queries */}
      {selectedDataset && !hasSearched && (
        <div className="max-w-2xl">
          <p className="text-sm text-muted-foreground mb-3 flex items-center gap-2">
            <Sparkles className="w-4 h-4" />
            Try an example:
          </p>
          <div className="flex flex-wrap gap-2">
            {exampleQueries.map((query, index) => (
              <button
                key={index}
                onClick={() => handleExampleClick(query)}
                className="px-3 py-1.5 text-sm rounded-full bg-secondary border border-border text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
              >
                {query}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Loading State */}
      {isSearching && (
        <Card className="bg-card border-border">
          <CardContent className="py-12">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <Loader2 className="w-12 h-12 text-primary animate-spin" />
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">
                  Searching...
                </h3>
                <p className="text-muted-foreground max-w-sm">
                  Analyzing your query and finding relevant results
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error Message */}
      {!isSearching && hasSearched && error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* No Results State */}
      {!isSearching && hasSearched && results && results.results.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-12">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                <Search className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">
                  No results found
                </h3>
                <p className="text-muted-foreground max-w-sm">
                  No results found for "<span className="text-foreground">{searchQuery}</span>". Try different search terms.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Search Results */}
      {!isSearching && results && results.results.length > 0 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-foreground">
              Found {results.total} results for "{results.query}"
            </h2>
            <span className="text-sm text-muted-foreground">
              {results.datasets_searched} dataset(s) searched in {results.duration_ms.toFixed(0)}ms
            </span>
          </div>

          <div className="space-y-3">
            {results.results.map((result, index) => (
              <Card
                key={`${result.dataset_id}-${result.row_index}-${index}`}
                className="bg-card border-border hover:border-primary/30 transition-colors"
              >
                <CardContent className="p-4">
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 space-y-3">
                      {/* Relevance Score */}
                      <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2 min-w-[100px]">
                          <span className="text-sm font-medium text-foreground">
                            {(result.score * 100).toFixed(1)}% match
                          </span>
                        </div>
                        <Progress
                          value={result.score * 100}
                          className="h-2 flex-1 max-w-[200px] bg-secondary"
                          indicatorClassName={getRelevanceColor(result.score)}
                        />
                        <span className="text-xs text-muted-foreground">
                          Row #{result.row_index}
                        </span>
                      </div>

                      {/* Dataset Name */}
                      <div className="text-xs text-muted-foreground">
                        From: {result.dataset_name}
                      </div>

                      {/* Text Content */}
                      <p className="text-sm text-muted-foreground line-clamp-2">
                        {result.text_content}
                      </p>

                      {/* Preview Fields */}
                      {result.row_data && Object.keys(result.row_data).length > 0 && (
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                          {Object.entries(result.row_data).slice(0, 4).map(([key, value]) => (
                            <div key={key} className="space-y-1">
                              <p className="text-xs text-muted-foreground">{key}</p>
                              <p className="text-sm text-foreground font-medium truncate">
                                {String(value)}
                              </p>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>

                    <Button
                      variant="outline"
                      size="sm"
                      className="shrink-0"
                      onClick={() => setSelectedRecord(result)}
                    >
                      <ExternalLink className="w-4 h-4 mr-2" />
                      View Full Record
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}

      {/* Empty State - No Dataset Selected */}
      {!selectedDataset && !isSearching && !datasetsLoading && datasets.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                <Database className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">
                  Select a dataset to search
                </h3>
                <p className="text-muted-foreground max-w-sm">
                  Choose a dataset from the dropdown above to start searching
                  with natural language queries.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Ready State - Dataset Selected but no search yet */}
      {selectedDataset && !isSearching && !hasSearched && (
        <Card className="bg-card border-border">
          <CardContent className="py-12">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                <Search className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-semibold text-foreground">
                  Ready to search
                </h3>
                <p className="text-muted-foreground max-w-sm">
                  Searching in <span className="text-foreground font-medium">{selectedDataset.name}</span>.
                  Enter a natural language query above to find relevant data.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Full Record Modal */}
      <Dialog open={!!selectedRecord} onOpenChange={() => setSelectedRecord(null)}>
        <DialogContent className="bg-card border-border max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center justify-between">
              <span>Full Record - Row #{selectedRecord?.row_index}</span>
            </DialogTitle>
          </DialogHeader>
          
          {selectedRecord && (
            <div className="space-y-4">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <span>Dataset: {selectedRecord.dataset_name}</span>
                <span>•</span>
                <span>Score: {(selectedRecord.score * 100).toFixed(1)}%</span>
              </div>

              <div className="space-y-3">
                {Object.entries(selectedRecord.row_data || {}).map(([key, value]) => (
                  <div key={key} className="flex flex-col gap-1 p-3 bg-secondary rounded-lg">
                    <span className="text-xs text-muted-foreground font-medium">{key}</span>
                    <span className="text-sm text-foreground">{String(value)}</span>
                  </div>
                ))}
              </div>

              <div className="flex gap-2 pt-4 border-t border-border">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleCopyJson(selectedRecord.row_data)}
                  className="gap-2"
                >
                  <Copy className="w-4 h-4" />
                  Copy as JSON
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleOpenInSqlQuery}
                  className="gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open in SQL Query
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default SearchPage;
