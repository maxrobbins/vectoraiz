/**
 * BQ-VZ-SHARED-SEARCH: Portal Search Page
 *
 * Search bar + dataset picker + results table.
 * Only shows display_columns — column restriction enforced server-side.
 */

import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Search, Loader2, Database, AlertCircle } from "lucide-react";
import {
  portalApi,
  type PortalDataset,
  type PortalSearchResponse,
} from "@/api/portalApi";

const PortalSearch = () => {
  const [searchParams] = useSearchParams();
  const datasetParam = searchParams.get("dataset");

  const [datasets, setDatasets] = useState<PortalDataset[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>("");
  const [query, setQuery] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const [results, setResults] = useState<PortalSearchResponse | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load datasets
  useEffect(() => {
    (async () => {
      try {
        const res = await portalApi.getDatasets();
        setDatasets(res.datasets);
        // Auto-select from URL param or first dataset
        if (datasetParam && res.datasets.some((d) => d.dataset_id === datasetParam)) {
          setSelectedDatasetId(datasetParam);
        } else if (res.datasets.length === 1) {
          setSelectedDatasetId(res.datasets[0].dataset_id);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load datasets");
      } finally {
        setDatasetsLoading(false);
      }
    })();
  }, [datasetParam]);

  const handleSearch = async () => {
    if (!query.trim() || !selectedDatasetId) return;

    setIsSearching(true);
    setError(null);
    setHasSearched(true);

    try {
      const res = await portalApi.search(selectedDatasetId, query.trim());
      setResults(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Search failed");
      setResults(null);
    } finally {
      setIsSearching(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && selectedDatasetId) handleSearch();
  };

  const getRelevanceColor = (score: number) => {
    const pct = score * 100;
    if (pct >= 90) return "bg-green-500";
    if (pct >= 75) return "bg-primary";
    return "bg-yellow-500";
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-foreground mb-1">Search</h2>
        <p className="text-muted-foreground text-sm">
          Search across shared datasets using natural language
        </p>
      </div>

      {/* Dataset Selector */}
      <div className="max-w-md">
        <label className="text-sm font-medium text-foreground mb-2 block">
          Dataset
        </label>
        {datasetsLoading ? (
          <Skeleton className="h-10 w-full" />
        ) : datasets.length === 0 ? (
          <div className="text-sm text-muted-foreground p-3 border border-dashed rounded-lg">
            No datasets available.
          </div>
        ) : (
          <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
            <SelectTrigger className="bg-secondary border-border">
              <SelectValue placeholder="Choose a dataset..." />
            </SelectTrigger>
            <SelectContent className="bg-card border-border z-50">
              {datasets.map((ds) => (
                <SelectItem key={ds.dataset_id} value={ds.dataset_id}>
                  {ds.name}
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
            placeholder="Search using natural language..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            className="pl-10 h-12 bg-secondary border-border text-foreground placeholder:text-muted-foreground"
            disabled={!selectedDatasetId}
          />
        </div>
        <Button
          className="h-12 px-6"
          disabled={!selectedDatasetId || isSearching || !query.trim()}
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

      {/* Error */}
      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Loading */}
      {isSearching && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Loader2 className="w-12 h-12 text-primary animate-spin mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-foreground">Searching...</h3>
          </CardContent>
        </Card>
      )}

      {/* No Results */}
      {!isSearching && hasSearched && results && results.results.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Search className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-foreground">No results found</h3>
            <p className="text-muted-foreground text-sm mt-1">
              Try different search terms.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Results */}
      {!isSearching && results && results.results.length > 0 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold text-foreground">
              {results.total_count} result{results.total_count !== 1 ? "s" : ""} for &ldquo;{results.query}&rdquo;
            </h3>
            <span className="text-sm text-muted-foreground">
              in {results.dataset_name}
            </span>
          </div>

          <div className="space-y-3">
            {results.results.map((r, idx) => (
              <Card key={idx} className="bg-card border-border">
                <CardContent className="p-4 space-y-3">
                  {/* Score */}
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-medium text-foreground min-w-[90px]">
                      {(r.score * 100).toFixed(1)}% match
                    </span>
                    <Progress
                      value={r.score * 100}
                      className="h-2 flex-1 max-w-[200px] bg-secondary"
                      indicatorClassName={getRelevanceColor(r.score)}
                    />
                  </div>

                  {/* Text content */}
                  {r.text_content && (
                    <p className="text-sm text-muted-foreground line-clamp-2">
                      {r.text_content}
                    </p>
                  )}

                  {/* Row data */}
                  {r.row_data && Object.keys(r.row_data).length > 0 && (
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                      {Object.entries(r.row_data).slice(0, 8).map(([key, value]) => (
                        <div key={key} className="space-y-1">
                          <p className="text-xs text-muted-foreground">{key}</p>
                          <p className="text-sm text-foreground font-medium truncate">
                            {String(value ?? "")}
                          </p>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}

      {/* Empty state */}
      {!isSearching && !hasSearched && selectedDatasetId && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Search className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-foreground">Ready to search</h3>
            <p className="text-muted-foreground text-sm mt-1">
              Enter a query above to find relevant data.
            </p>
          </CardContent>
        </Card>
      )}

      {/* No dataset selected */}
      {!isSearching && !selectedDatasetId && !datasetsLoading && datasets.length > 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Database className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-foreground">Select a dataset</h3>
            <p className="text-muted-foreground text-sm mt-1">
              Choose a dataset from the dropdown to start searching.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default PortalSearch;
