import { useState, useEffect, useRef, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { 
  Play, 
  Code, 
  Trash2, 
  Save, 
  ChevronLeft, 
  ChevronRight,
  Hash,
  Type,
  Calendar,
  ToggleLeft,
  Download,
  FileText,
  ChevronDown,
  ArrowUpDown,
  Bookmark,
  Percent,
  AlertCircle,
  Loader2,
  X
} from "lucide-react";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
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
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { toast } from "@/hooks/use-toast";
import { useSQLQuery, useSQLTables, useDatasets, useDataset } from "@/hooks/useApi";
import { databaseApi, DatabaseConnection, DirectQueryResponse } from "@/api/database";

// Saved query type
interface SavedQuery {
  id: string;
  name: string;
  query: string;
  createdAt: string;
}

const SAVED_QUERIES_KEY = "vectoraiz_saved_queries";

// Load saved queries from localStorage
const loadSavedQueries = (): SavedQuery[] => {
  try {
    const stored = localStorage.getItem(SAVED_QUERIES_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
};

// Save queries to localStorage
const persistSavedQueries = (queries: SavedQuery[]) => {
  localStorage.setItem(SAVED_QUERIES_KEY, JSON.stringify(queries));
};

type DataType = "string" | "integer" | "float" | "date" | "boolean";

const getDataTypeIcon = (dataType: DataType) => {
  switch (dataType) {
    case "integer":
      return <Hash className="w-3.5 h-3.5" />;
    case "float":
      return <Percent className="w-3.5 h-3.5" />;
    case "string":
      return <Type className="w-3.5 h-3.5" />;
    case "date":
      return <Calendar className="w-3.5 h-3.5" />;
    case "boolean":
      return <ToggleLeft className="w-3.5 h-3.5" />;
    default:
      return <Type className="w-3.5 h-3.5" />;
  }
};

// Map API column type to display type
const mapApiTypeToDisplayType = (apiType: string | undefined): DataType => {
  if (!apiType) return "string";
  const type = apiType.toLowerCase();
  if (type.includes("int") || type.includes("bigint")) return "integer";
  if (type.includes("float") || type.includes("double") || type.includes("decimal") || type.includes("number")) return "float";
  if (type.includes("date") || type.includes("time")) return "date";
  if (type.includes("bool")) return "boolean";
  return "string";
};

type QuerySource = "dataset" | "connection";

const SqlQuery = () => {
  const [searchParams] = useSearchParams();
  const datasetIdParam = searchParams.get("dataset");

  const [querySource, setQuerySource] = useState<QuerySource>(datasetIdParam ? "dataset" : "dataset");
  const [selectedDataset, setSelectedDataset] = useState<string>(datasetIdParam || "");
  const [selectedConnection, setSelectedConnection] = useState<string>("");
  const [dbConnections, setDbConnections] = useState<DatabaseConnection[]>([]);
  const [connectionsLoading, setConnectionsLoading] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [query, setQuery] = useState("SELECT * FROM dataset LIMIT 10");
  const [activeTab, setActiveTab] = useState("results");
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [queryName, setQueryName] = useState("");
  const [schemaExpanded, setSchemaExpanded] = useState(true);
  const [savedQueriesExpanded, setSavedQueriesExpanded] = useState(true);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(loadSavedQueries);

  // Direct query state (for connection mode)
  const [directResults, setDirectResults] = useState<DirectQueryResponse | null>(null);
  const [directLoading, setDirectLoading] = useState(false);
  const [directError, setDirectError] = useState<string | null>(null);
  const [directDuration, setDirectDuration] = useState<number | null>(null);

  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // API hooks
  const { execute, results: apiResults, loading: isExecuting, error: sqlError } = useSQLQuery();
  const { data: tablesData } = useSQLTables();
  const { data: datasetsData, loading: datasetsLoading } = useDatasets();
  const { data: selectedDatasetData, loading: schemaLoading } = useDataset(selectedDataset);

  // Load database connections
  useEffect(() => {
    setConnectionsLoading(true);
    databaseApi.list()
      .then(conns => setDbConnections(conns))
      .catch(() => {}) // Silently fail — connections are optional
      .finally(() => setConnectionsLoading(false));
  }, []);

  // Get datasets list from API
  const datasets = datasetsData?.datasets
    ?.filter(d => d.status === 'ready')
    ?.map(d => ({ id: d.id, name: d.original_filename })) || [];

  // Get schema columns from selected dataset API response
  const schemaColumns = selectedDatasetData?.metadata?.columns?.map(col => ({
    name: col.name,
    dataType: mapApiTypeToDisplayType(col.type),
  })) || [];

  // Auto-select dataset from URL parameter
  useEffect(() => {
    if (datasetIdParam && datasets.find(d => d.id === datasetIdParam)) {
      setSelectedDataset(datasetIdParam);
      setQuerySource("dataset");
    }
  }, [datasetIdParam, datasets]);

  const executeQuery = useCallback(async () => {
    if (!query.trim()) {
      toast({
        title: "Empty query",
        description: "Please enter a SQL query to execute.",
        variant: "destructive",
      });
      return;
    }

    if (querySource === "connection") {
      if (!selectedConnection) {
        toast({
          title: "No database selected",
          description: "Please select a database connection before running a query.",
          variant: "destructive",
        });
        return;
      }
      // Direct query against connected database
      setDirectLoading(true);
      setDirectError(null);
      setDirectResults(null);
      const start = performance.now();
      try {
        const result = await databaseApi.query(selectedConnection, { sql: query });
        setDirectDuration(performance.now() - start);
        setDirectResults(result);
        setActiveTab("results");
      } catch (err: any) {
        setDirectDuration(performance.now() - start);
        setDirectError(err.message || "Query failed");
        setActiveTab("messages");
      } finally {
        setDirectLoading(false);
      }
    } else {
      if (!selectedDataset) {
        toast({
          title: "No dataset selected",
          description: "Please select a dataset before running a query.",
          variant: "destructive",
        });
        return;
      }
      const result = await execute(query, { dataset_id: selectedDataset });
      if (result) {
        setActiveTab("results");
      } else {
        setActiveTab("messages");
      }
    }
  }, [query, querySource, selectedConnection, selectedDataset, execute]);

  // Keyboard shortcut handler
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        executeQuery();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [executeQuery]);

  const handleColumnClick = (columnName: string) => {
    const textarea = textareaRef.current;
    const defaultQuery = "SELECT * FROM dataset LIMIT 10";
    const trimmedQuery = query.trim();
    
    // Case 1: Editor is empty - insert full SELECT statement
    if (!trimmedQuery) {
      setQuery(`SELECT ${columnName} FROM dataset LIMIT 10`);
      setTimeout(() => textarea?.focus(), 0);
      return;
    }
    
    // Case 2: Editor has default text with * - replace * with column name
    if (trimmedQuery === defaultQuery) {
      setQuery(`SELECT ${columnName} FROM dataset LIMIT 10`);
      setTimeout(() => textarea?.focus(), 0);
      return;
    }
    
    // Case 3: Check if it's a SELECT statement where we can add columns
    const selectMatch = query.match(/^(SELECT\s+)([\w\s,]+?)(\s+FROM\s+.*)$/is);
    if (selectMatch) {
      const [, selectPart, columnsPart, fromPart] = selectMatch;
      const existingColumns = columnsPart.trim();
      
      // Check if the column is already in the list
      const columnList = existingColumns.split(',').map(c => c.trim().toLowerCase());
      if (columnList.includes(columnName.toLowerCase())) {
        toast({
          title: "Column already added",
          description: `${columnName} is already in the query.`,
        });
        setTimeout(() => textarea?.focus(), 0);
        return;
      }
      
      // Add the new column after existing columns
      const newQuery = `${selectPart}${existingColumns}, ${columnName}${fromPart}`;
      setQuery(newQuery);
      setTimeout(() => textarea?.focus(), 0);
      return;
    }
    
    // Case 4: Custom text - insert at cursor position
    if (textarea) {
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      
      let insertText = columnName;
      if (start > 0) {
        const charBefore = query.charAt(start - 1);
        if (charBefore !== ' ' && charBefore !== ',' && charBefore !== '\n' && charBefore !== '\t' && charBefore !== '(') {
          insertText = " " + columnName;
        }
      }
      if (end < query.length) {
        const charAfter = query.charAt(end);
        if (charAfter !== ' ' && charAfter !== ',' && charAfter !== '\n' && charAfter !== ')') {
          insertText = insertText + " ";
        }
      }
      
      const newQuery = query.slice(0, start) + insertText + query.slice(end);
      setQuery(newQuery);
      
      setTimeout(() => {
        textarea.focus();
        const newPosition = start + insertText.length;
        textarea.setSelectionRange(newPosition, newPosition);
      }, 0);
    }
  };

  const handleLoadSavedQuery = (savedQuery: SavedQuery) => {
    setQuery(savedQuery.query);
    toast({
      title: "Query loaded",
      description: `Loaded "${savedQuery.name}"`,
    });
  };

  const handleDeleteSavedQuery = (queryId: string) => {
    const updated = savedQueries.filter(q => q.id !== queryId);
    setSavedQueries(updated);
    persistSavedQueries(updated);
    toast({
      title: "Query deleted",
    });
  };

  const formatQuery = () => {
    const formatted = query
      .replace(/\s+/g, " ")
      .replace(/\s*,\s*/g, ", ")
      .replace(/\bSELECT\b/gi, "SELECT")
      .replace(/\bFROM\b/gi, "\nFROM")
      .replace(/\bWHERE\b/gi, "\nWHERE")
      .replace(/\bAND\b/gi, "\n  AND")
      .replace(/\bOR\b/gi, "\n  OR")
      .replace(/\bGROUP BY\b/gi, "\nGROUP BY")
      .replace(/\bORDER BY\b/gi, "\nORDER BY")
      .replace(/\bLIMIT\b/gi, "\nLIMIT")
      .replace(/\bJOIN\b/gi, "\nJOIN")
      .replace(/\bLEFT JOIN\b/gi, "\nLEFT JOIN")
      .replace(/\bRIGHT JOIN\b/gi, "\nRIGHT JOIN")
      .replace(/\bINNER JOIN\b/gi, "\nINNER JOIN")
      .trim();
    setQuery(formatted);
    toast({ title: "Query formatted" });
  };

  const clearQuery = () => {
    setQuery("");
  };

  const saveQuery = () => {
    if (!queryName.trim()) {
      toast({
        title: "Name required",
        description: "Please enter a name for your query.",
        variant: "destructive",
      });
      return;
    }
    
    const newQuery: SavedQuery = {
      id: Date.now().toString(),
      name: queryName.trim(),
      query: query,
      createdAt: new Date().toISOString(),
    };
    
    const updated = [...savedQueries, newQuery];
    setSavedQueries(updated);
    persistSavedQueries(updated);
    
    toast({
      title: "Query saved",
      description: `"${queryName}" has been saved.`,
    });
    setSaveDialogOpen(false);
    setQueryName("");
  };

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
  };

  // Derive results and execution info from API response (unified across modes)
  const isRunning = querySource === "connection" ? directLoading : isExecuting;

  const results = querySource === "connection"
    ? (directResults ? directResults.rows.map(row => {
        const obj: Record<string, any> = {};
        directResults.columns.forEach((col, i) => { obj[col] = row[i]; });
        return obj;
      }) : null)
    : (apiResults?.data || null);

  const executionTime = querySource === "connection"
    ? (directDuration ? (directDuration / 1000).toFixed(2) : null)
    : (apiResults?.duration_ms ? (apiResults.duration_ms / 1000).toFixed(2) : null);

  const errorMessage = querySource === "connection" ? directError : (sqlError || null);

  const sortedResults = results ? [...results].sort((a, b) => {
    if (!sortColumn) return 0;
    const aVal = a[sortColumn as keyof typeof a];
    const bVal = b[sortColumn as keyof typeof b];
    if (aVal === bVal) return 0;
    if (aVal === null) return 1;
    if (bVal === null) return -1;
    const comparison = aVal < bVal ? -1 : 1;
    return sortDirection === "asc" ? comparison : -comparison;
  }) : null;

  const exportData = (format: "csv" | "json" | "parquet") => {
    if (!results) return;
    
    if (format === "json") {
      const json = JSON.stringify(results, null, 2);
      const blob = new Blob([json], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "query_results.json";
      a.click();
      URL.revokeObjectURL(url);
    } else if (format === "csv") {
      const headers = Object.keys(results[0]).join(",");
      const rows = results.map(row => 
        Object.values(row).map(v => `"${v}"`).join(",")
      ).join("\n");
      const csv = headers + "\n" + rows;
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "query_results.csv";
      a.click();
      URL.revokeObjectURL(url);
    } else {
      toast({
        title: "Parquet export",
        description: "Parquet export would be available in production.",
      });
    }
  };

  return (
    <div className="flex h-[calc(100vh-120px)] gap-4">
      {/* Left Panel - Schema Browser */}
      <div 
        className={`${sidebarCollapsed ? "w-12" : "w-[280px]"} transition-all duration-300 flex flex-col bg-card border-r border-border flex-shrink-0 overflow-hidden`}
      >
        <div className="flex items-center justify-between p-3 border-b border-border">
          {!sidebarCollapsed && (
            <span className="font-medium text-sm text-foreground">Explorer</span>
          )}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            className="h-7 w-7"
          >
            {sidebarCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
          </Button>
        </div>

        {!sidebarCollapsed && (
          <div className="flex-1 overflow-y-auto p-3 space-y-4">
            {/* Source Mode Selector */}
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground uppercase tracking-wider">Source</Label>
              <Select value={querySource} onValueChange={(v) => setQuerySource(v as QuerySource)}>
                <SelectTrigger className="bg-background border-border">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="dataset">Local Datasets</SelectItem>
                  <SelectItem value="connection">Database Connections</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Dataset Selector (dataset mode) */}
            {querySource === "dataset" && (
              <div className="space-y-2">
                <Label className="text-xs text-muted-foreground uppercase tracking-wider">Dataset</Label>
                {datasetsLoading ? (
                  <Skeleton className="h-10 w-full" />
                ) : datasets.length === 0 ? (
                  <p className="text-xs text-muted-foreground">No datasets available</p>
                ) : (
                  <Select value={selectedDataset} onValueChange={setSelectedDataset}>
                    <SelectTrigger className="bg-background border-border">
                      <SelectValue placeholder="Select dataset" />
                    </SelectTrigger>
                    <SelectContent>
                      {datasets.map((dataset) => (
                        <SelectItem key={dataset.id} value={dataset.id}>
                          {dataset.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              </div>
            )}

            {/* Database Connection Selector (connection mode) */}
            {querySource === "connection" && (
              <div className="space-y-2">
                <Label className="text-xs text-muted-foreground uppercase tracking-wider">Connection</Label>
                {connectionsLoading ? (
                  <Skeleton className="h-10 w-full" />
                ) : dbConnections.length === 0 ? (
                  <p className="text-xs text-muted-foreground">No database connections configured. Add one in the Database page.</p>
                ) : (
                  <Select value={selectedConnection} onValueChange={setSelectedConnection}>
                    <SelectTrigger className="bg-background border-border">
                      <SelectValue placeholder="Select connection" />
                    </SelectTrigger>
                    <SelectContent>
                      {dbConnections.map((conn) => (
                        <SelectItem key={conn.id} value={conn.id}>
                          <div className="flex items-center gap-2">
                            <span className={`w-2 h-2 rounded-full ${conn.status === "connected" ? "bg-green-500" : conn.status === "error" ? "bg-red-500" : "bg-yellow-500"}`} />
                            {conn.name}
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              </div>
            )}

            {/* Schema Browser (dataset mode only) */}
            {querySource === "dataset" && selectedDataset && (
              <div className="space-y-2">
                <button
                  onClick={() => setSchemaExpanded(!schemaExpanded)}
                  className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider hover:text-foreground w-full"
                >
                  {schemaExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                  Schema
                </button>
                
                {schemaExpanded && (
                  <div className="space-y-0.5 ml-2">
                    {schemaLoading ? (
                      <div className="space-y-1">
                        <Skeleton className="h-7 w-full" />
                        <Skeleton className="h-7 w-full" />
                        <Skeleton className="h-7 w-full" />
                      </div>
                    ) : schemaColumns.length === 0 ? (
                      <p className="text-xs text-muted-foreground px-2 py-1">No schema available</p>
                    ) : (
                      schemaColumns.map((column) => (
                        <Tooltip key={column.name}>
                          <TooltipTrigger asChild>
                            <button
                              onClick={() => handleColumnClick(column.name)}
                              className="flex items-center gap-2 w-full px-2 py-1.5 text-sm hover:bg-secondary rounded transition-colors group"
                            >
                              <span className="text-primary flex-shrink-0">{getDataTypeIcon(column.dataType)}</span>
                              <span className="truncate text-foreground group-hover:text-foreground">{column.name}</span>
                              <span className="text-xs text-muted-foreground ml-auto flex-shrink-0">{column.dataType}</span>
                            </button>
                          </TooltipTrigger>
                          <TooltipContent side="right">
                            <p>Click to add to query</p>
                          </TooltipContent>
                        </Tooltip>
                      ))
                    )}
                  </div>
                )}
              </div>
            )}

            {/* Saved Queries */}
            <div className="space-y-2">
              <button
                onClick={() => setSavedQueriesExpanded(!savedQueriesExpanded)}
                className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider hover:text-foreground w-full"
              >
                {savedQueriesExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                Saved Queries ({savedQueries.length})
              </button>
              
              {savedQueriesExpanded && (
                <div className="space-y-1 ml-2">
                  {savedQueries.length === 0 ? (
                    <p className="text-xs text-muted-foreground px-2 py-1">No saved queries</p>
                  ) : (
                    savedQueries.map((saved) => (
                      <div
                        key={saved.id}
                        className="flex items-center gap-1 group"
                      >
                        <button
                          onClick={() => handleLoadSavedQuery(saved)}
                          className="flex items-center gap-2 flex-1 px-2 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-secondary rounded transition-colors"
                        >
                          <Bookmark className="w-3 h-3 text-primary" />
                          <span className="truncate">{saved.name}</span>
                        </button>
                        <button
                          onClick={() => handleDeleteSavedQuery(saved.id)}
                          className="p-1 opacity-0 group-hover:opacity-100 hover:text-destructive transition-opacity"
                        >
                          <X className="w-3 h-3" />
                        </button>
                      </div>
                    ))
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Main Panel */}
      <div className="flex-1 flex flex-col gap-4 min-w-0">
        {/* Toolbar */}
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2">
            <Button
              onClick={executeQuery}
              disabled={isRunning || (querySource === "dataset" ? !selectedDataset : !selectedConnection)}
              className="gap-2 bg-[hsl(var(--haven-success))] hover:bg-[hsl(var(--haven-success))]/90"
            >
              {isRunning ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              {isRunning ? "Executing..." : "Run"}
            </Button>
            <Button variant="outline" onClick={formatQuery} className="gap-2">
              <Code className="w-4 h-4" />
              Format
            </Button>
            <Button variant="outline" onClick={clearQuery} className="gap-2">
              <Trash2 className="w-4 h-4" />
              Clear
            </Button>
            <Button variant="outline" onClick={() => setSaveDialogOpen(true)} className="gap-2">
              <Save className="w-4 h-4" />
              Save Query
            </Button>
          </div>
          <span className="text-xs text-muted-foreground">
            Ctrl+Enter to run
          </span>
        </div>

        {/* Query Editor */}
        <Card className="bg-card border-border flex-shrink-0">
          <CardContent className="p-0">
            <div className="relative">
              {/* Line numbers */}
              <div className="absolute left-0 top-0 bottom-0 w-10 bg-secondary/50 border-r border-border flex flex-col items-end pr-2 pt-3 text-xs text-muted-foreground font-mono select-none">
                {query.split('\n').map((_, i) => (
                  <div key={i} className="h-[1.5rem] leading-[1.5rem]">{i + 1}</div>
                ))}
              </div>
              <textarea
                ref={textareaRef}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="w-full min-h-[150px] bg-transparent text-foreground font-mono text-sm p-3 pl-12 resize-none focus:outline-none leading-[1.5rem]"
                spellCheck={false}
              />
            </div>
          </CardContent>
        </Card>

        {/* Results Panel */}
        <Card className="bg-card border-border flex-1 flex flex-col overflow-hidden">
          <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col">
            <div className="flex items-center justify-between px-4 pt-3 pb-2 border-b border-border">
              <TabsList className="bg-secondary/50">
                <TabsTrigger value="results">Results</TabsTrigger>
                <TabsTrigger value="messages">Messages</TabsTrigger>
              </TabsList>
              
              {results && results.length > 0 && (
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="outline" size="sm" className="gap-2">
                      <Download className="w-4 h-4" />
                      Export
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent>
                    <DropdownMenuItem onClick={() => exportData("csv")}>
                      <FileText className="w-4 h-4 mr-2" />
                      Export as CSV
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => exportData("json")}>
                      <FileText className="w-4 h-4 mr-2" />
                      Export as JSON
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => exportData("parquet")}>
                      <FileText className="w-4 h-4 mr-2" />
                      Export as Parquet
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              )}
            </div>

            <TabsContent value="results" className="flex-1 overflow-auto m-0 p-4">
              {sortedResults && sortedResults.length > 0 ? (
                <div className="space-y-4">
                  <div className="overflow-x-auto rounded-lg border border-border">
                    <Table>
                      <TableHeader>
                        <TableRow className="hover:bg-transparent">
                          {Object.keys(sortedResults[0]).map((key) => (
                            <TableHead 
                              key={key}
                              className="cursor-pointer hover:bg-secondary/50 transition-colors"
                              onClick={() => handleSort(key)}
                            >
                              <div className="flex items-center gap-1">
                                {key}
                                <ArrowUpDown className="w-3 h-3 opacity-50" />
                                {sortColumn === key && (
                                  <span className="text-primary text-xs">
                                    {sortDirection === "asc" ? "↑" : "↓"}
                                  </span>
                                )}
                              </div>
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {sortedResults.map((row, i) => (
                          <TableRow key={i}>
                            {Object.values(row).map((value, j) => (
                              <TableCell key={j} className="whitespace-nowrap">
                                {value === null ? (
                                  <span className="text-muted-foreground italic">null</span>
                                ) : typeof value === "number" ? (
                                  value.toLocaleString()
                                ) : (
                                  String(value)
                                )}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    Showing {sortedResults.length} rows (query executed in {executionTime}s)
                    {querySource === "connection" && directResults?.truncated && (
                      <span className="text-yellow-500 ml-2">— Results truncated at {directResults.row_count} rows</span>
                    )}
                  </div>
                </div>
              ) : isRunning ? (
                <div className="flex items-center justify-center h-32">
                  <Loader2 className="w-6 h-6 animate-spin text-primary" />
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-32 text-center space-y-2">
                  <Code className="w-8 h-8 text-muted-foreground" />
                  <p className="text-muted-foreground">Run a query to see results</p>
                </div>
              )}
            </TabsContent>

            <TabsContent value="messages" className="flex-1 overflow-auto m-0 p-4">
              <div className="space-y-2 font-mono text-sm">
                {errorMessage ? (
                  <div className="p-3 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive">
                    ✗ Error: {errorMessage}
                  </div>
                ) : executionTime ? (
                  <div className="p-3 bg-[hsl(var(--haven-success))]/10 border border-[hsl(var(--haven-success))]/30 rounded-lg text-[hsl(var(--haven-success))]">
                    ✓ Query executed successfully. {results?.length || 0} rows returned in {executionTime}s
                  </div>
                ) : (
                  <div className="text-muted-foreground">
                    No messages yet. Run a query to see execution info.
                  </div>
                )}
              </div>
            </TabsContent>
          </Tabs>
        </Card>
      </div>

      {/* Save Query Dialog */}
      <Dialog open={saveDialogOpen} onOpenChange={setSaveDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Save Query</DialogTitle>
            <DialogDescription>
              Give your query a name to save it for later use.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Label htmlFor="query-name">Query Name</Label>
            <Input
              id="query-name"
              value={queryName}
              onChange={(e) => setQueryName(e.target.value)}
              placeholder="e.g., Top Customers by Revenue"
              className="mt-2"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSaveDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={saveQuery}>Save Query</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default SqlQuery;
