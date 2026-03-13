import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Database,
  Plus,
  Trash2,
  ArrowLeft,
  Loader2,
  CheckCircle,
  XCircle,
  AlertCircle,
  RefreshCw,
  ChevronRight,
  ChevronDown,
  Eye,
  EyeOff,
  Server,
  Cable,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Textarea } from "@/components/ui/textarea";
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
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import { toast } from "@/hooks/use-toast";
import {
  databaseApi,
  type DatabaseConnection,
  type ConnectionCreateRequest,
  type TableInfo,
  type TestConnectionResult,
} from "@/api/database";
import { cn } from "@/lib/utils";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type View = "list" | "form" | "schema";

interface FormState {
  name: string;
  db_type: "postgresql" | "mysql";
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  ssl_mode: "disable" | "prefer" | "require";
}

const DEFAULT_FORM: FormState = {
  name: "",
  db_type: "postgresql",
  host: "localhost",
  port: "5432",
  database: "",
  username: "",
  password: "",
  ssl_mode: "prefer",
};

const formatDate = (iso: string | null): string => {
  if (!iso) return "Never";
  return new Date(iso).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
};

const formatNumber = (n: number): string => n.toLocaleString();

const statusBadge = (status: string) => {
  switch (status) {
    case "connected":
      return (
        <Badge variant="secondary" className="bg-haven-success/20 text-haven-success border-haven-success/30 gap-1">
          <div className="w-1.5 h-1.5 rounded-full bg-haven-success" />
          Connected
        </Badge>
      );
    case "error":
      return (
        <Badge variant="secondary" className="bg-destructive/20 text-destructive border-destructive/30 gap-1">
          <div className="w-1.5 h-1.5 rounded-full bg-destructive" />
          Error
        </Badge>
      );
    default:
      return (
        <Badge variant="secondary" className="bg-muted text-muted-foreground gap-1">
          <div className="w-1.5 h-1.5 rounded-full bg-muted-foreground" />
          Not Tested
        </Badge>
      );
  }
};

// ===========================================================================
// Main Page Component
// ===========================================================================

const DatabasePage = () => {
  const navigate = useNavigate();

  // -- View state --
  const [view, setView] = useState<View>("list");
  const [editingId, setEditingId] = useState<string | null>(null);
  const [selectedConnectionId, setSelectedConnectionId] = useState<string | null>(null);

  // -- Connections list --
  const [connections, setConnections] = useState<DatabaseConnection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // -- Form state --
  const [form, setForm] = useState<FormState>({ ...DEFAULT_FORM });
  const [showPassword, setShowPassword] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null);

  // -- Schema browser state --
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [schemaError, setSchemaError] = useState<string | null>(null);
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [showCustomSql, setShowCustomSql] = useState(false);
  const [customSql, setCustomSql] = useState("");
  const [customSqlName, setCustomSqlName] = useState("");
  const [extracting, setExtracting] = useState(false);

  // -----------------------------------------------------------------------
  // Fetch connections
  // -----------------------------------------------------------------------

  const fetchConnections = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await databaseApi.list().catch(() => [] as DatabaseConnection[]);
      setConnections(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load connections");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchConnections();
  }, [fetchConnections]);

  // -----------------------------------------------------------------------
  // Navigation helpers
  // -----------------------------------------------------------------------

  const goToList = () => {
    setView("list");
    setEditingId(null);
    setTestResult(null);
    setForm({ ...DEFAULT_FORM });
    setShowPassword(false);
  };

  const goToForm = (connection?: DatabaseConnection) => {
    if (connection) {
      setEditingId(connection.id);
      setForm({
        name: connection.name,
        db_type: connection.db_type as "postgresql" | "mysql",
        host: connection.host,
        port: String(connection.port),
        database: connection.database,
        username: connection.username,
        password: "",
        ssl_mode: connection.ssl_mode as "disable" | "prefer" | "require",
      });
    } else {
      setEditingId(null);
      setForm({ ...DEFAULT_FORM });
    }
    setTestResult(null);
    setShowPassword(false);
    setView("form");
  };

  const goToSchema = async (connectionId: string) => {
    setSelectedConnectionId(connectionId);
    setView("schema");
    setTables([]);
    setSelectedTables(new Set());
    setExpandedTables(new Set());
    setShowCustomSql(false);
    setCustomSql("");
    setCustomSqlName("");
    setSchemaError(null);
    setSchemaLoading(true);

    try {
      const resp = await databaseApi.schema(connectionId);
      setTables(resp.tables);
      if (resp.warning) {
        setSchemaError(resp.warning);
      }
    } catch (e) {
      setSchemaError(e instanceof Error ? e.message : "Failed to load schema");
    } finally {
      setSchemaLoading(false);
    }
  };

  // -----------------------------------------------------------------------
  // Form: type change → auto-fill port
  // -----------------------------------------------------------------------

  const handleTypeChange = (dbType: "postgresql" | "mysql") => {
    const autoPort = dbType === "postgresql" ? "5432" : "3306";
    const currentIsDefault = form.port === "5432" || form.port === "3306" || form.port === "";
    setForm((f) => ({
      ...f,
      db_type: dbType,
      port: currentIsDefault ? autoPort : f.port,
    }));
  };

  // -----------------------------------------------------------------------
  // Form: Test connection
  // -----------------------------------------------------------------------

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);

    try {
      // Need a saved connection to test — save first if new
      let connId = editingId;
      if (!connId) {
        const created = await databaseApi.create({
          ...form,
          port: parseInt(form.port, 10),
        } as ConnectionCreateRequest);
        connId = created.id;
        setEditingId(connId);
      } else {
        // Update existing
        await databaseApi.update(connId, {
          ...form,
          port: parseInt(form.port, 10),
          password: form.password || undefined,
        });
      }

      const result = await databaseApi.test(connId);
      setTestResult(result);
      fetchConnections();

      if (result.ok) {
        toast({ title: "Connection successful", description: `Latency: ${result.latency_ms}ms` });
      } else {
        toast({ title: "Connection failed", description: result.error, variant: "destructive" });
      }
    } catch (e) {
      toast({
        title: "Test failed",
        description: e instanceof Error ? e.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setTesting(false);
    }
  };

  // -----------------------------------------------------------------------
  // Form: Save
  // -----------------------------------------------------------------------

  const handleSave = async () => {
    if (!form.name || !form.host || !form.database || !form.username) {
      toast({ title: "Validation error", description: "Please fill in all required fields.", variant: "destructive" });
      return;
    }
    if (!editingId && !form.password) {
      toast({ title: "Validation error", description: "Password is required for new connections.", variant: "destructive" });
      return;
    }

    setSaving(true);
    try {
      if (editingId) {
        await databaseApi.update(editingId, {
          ...form,
          port: parseInt(form.port, 10),
          password: form.password || undefined,
        });
        toast({ title: "Connection updated" });
      } else {
        await databaseApi.create({
          ...form,
          port: parseInt(form.port, 10),
        } as ConnectionCreateRequest);
        toast({ title: "Connection created" });
      }
      await fetchConnections();
      goToList();
    } catch (e) {
      toast({
        title: "Save failed",
        description: e instanceof Error ? e.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  // -----------------------------------------------------------------------
  // Delete connection
  // -----------------------------------------------------------------------

  const handleDelete = async (id: string) => {
    try {
      await databaseApi.delete(id);
      toast({ title: "Connection deleted" });
      fetchConnections();
    } catch (e) {
      toast({
        title: "Delete failed",
        description: e instanceof Error ? e.message : "Unknown error",
        variant: "destructive",
      });
    }
  };

  // -----------------------------------------------------------------------
  // Schema: toggle table expand
  // -----------------------------------------------------------------------

  const toggleTableExpand = (tableName: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev);
      if (next.has(tableName)) next.delete(tableName);
      else next.add(tableName);
      return next;
    });
  };

  const toggleTableSelect = (tableName: string) => {
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(tableName)) next.delete(tableName);
      else next.add(tableName);
      return next;
    });
  };

  // -----------------------------------------------------------------------
  // Schema: extract
  // -----------------------------------------------------------------------

  const handleExtract = async () => {
    if (!selectedConnectionId) return;

    const hasCustomSql = showCustomSql && customSql.trim();
    const hasSelectedTables = selectedTables.size > 0;

    if (!hasCustomSql && !hasSelectedTables) {
      toast({ title: "Nothing selected", description: "Select tables or write custom SQL to extract.", variant: "destructive" });
      return;
    }

    if (hasCustomSql && !customSqlName.trim()) {
      toast({ title: "Name required", description: "Provide a dataset name for custom SQL extraction.", variant: "destructive" });
      return;
    }

    setExtracting(true);
    try {
      const result = await databaseApi.extract(selectedConnectionId, {
        tables: hasSelectedTables
          ? Array.from(selectedTables).map((name) => {
              const tbl = tables.find((t) => t.name === name);
              return { table: name, schema: tbl?.schema };
            })
          : undefined,
        custom_sql: hasCustomSql ? customSql : undefined,
        dataset_name: hasCustomSql ? customSqlName : undefined,
      });

      toast({
        title: "Extraction started",
        description: result.message,
      });

      // Navigate to datasets page to see progress
      navigate("/datasets");
    } catch (e) {
      toast({
        title: "Extraction failed",
        description: e instanceof Error ? e.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setExtracting(false);
    }
  };

  // -----------------------------------------------------------------------
  // Find selected connection for schema view
  // -----------------------------------------------------------------------

  const selectedConnection = connections.find((c) => c.id === selectedConnectionId);

  // =======================================================================
  // RENDER: Connections List
  // =======================================================================

  if (view === "list") {
    return (
      <div className="space-y-6 animate-in fade-in duration-300">
        {/* Header */}
        <div className="flex items-center justify-between">
          <p className="text-muted-foreground">
            Connect to external databases to import data into vectorAIz
          </p>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={fetchConnections} className="gap-2">
              <RefreshCw className="w-4 h-4" />
              Refresh
            </Button>
            <Button className="gap-2" onClick={() => goToForm()}>
              <Plus className="w-4 h-4" />
              Add Connection
            </Button>
          </div>
        </div>

        {/* Error */}
        {error && (
          <Card className="border-destructive/50 bg-destructive/5">
            <CardContent className="py-4 flex items-center gap-3">
              <AlertCircle className="w-5 h-5 text-destructive flex-shrink-0" />
              <p className="text-sm text-destructive">{error}</p>
              <Button variant="link" size="sm" className="ml-auto" onClick={fetchConnections}>
                Retry
              </Button>
            </CardContent>
          </Card>
        )}

        {/* Loading */}
        {loading && (
          <div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-20 rounded-lg" />
            ))}
          </div>
        )}

        {/* Empty State */}
        {!loading && !error && connections.length === 0 && (
          <Card className="bg-card border-border border-dashed">
            <CardContent className="py-16">
              <div className="flex flex-col items-center justify-center text-center space-y-4">
                <div className="w-16 h-16 rounded-full bg-secondary flex items-center justify-center">
                  <Cable className="w-8 h-8 text-muted-foreground" />
                </div>
                <div className="space-y-2">
                  <h3 className="text-lg font-semibold text-foreground">No database connections yet</h3>
                  <p className="text-muted-foreground max-w-sm">
                    Connect your first database to import data directly. Supports PostgreSQL and MySQL.
                  </p>
                </div>
                <Button variant="secondary" className="gap-2 mt-2" onClick={() => goToForm()}>
                  <Plus className="w-4 h-4" />
                  Connect your first database
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Connections List */}
        {!loading && connections.length > 0 && (
          <div className="space-y-3">
            {connections.map((conn) => (
              <Card key={conn.id} className="bg-card border-border hover:bg-secondary/30 transition-colors">
                <CardContent className="py-4">
                  <div className="flex items-center gap-4">
                    {/* Icon */}
                    <div className="w-10 h-10 rounded-lg bg-secondary flex items-center justify-center flex-shrink-0">
                      <Server className="w-5 h-5 text-primary" />
                    </div>

                    {/* Info */}
                    <div
                      className="flex-1 min-w-0 cursor-pointer"
                      onClick={() => goToSchema(conn.id)}
                    >
                      <div className="flex items-center gap-2">
                        <h3 className="font-medium text-foreground truncate">{conn.name}</h3>
                        {statusBadge(conn.status)}
                      </div>
                      <p className="text-sm text-muted-foreground mt-0.5">
                        {conn.db_type === "postgresql" ? "PostgreSQL" : "MySQL"} &middot;{" "}
                        {conn.host}:{conn.port}/{conn.database}
                        {conn.table_count != null && ` \u00b7 ${conn.table_count} tables`}
                      </p>
                      {conn.error_message && (
                        <p className="text-xs text-destructive mt-1 truncate">{conn.error_message}</p>
                      )}
                    </div>

                    {/* Meta */}
                    <div className="text-right text-xs text-muted-foreground hidden md:block flex-shrink-0">
                      <p>Last connected: {formatDate(conn.last_connected_at)}</p>
                      {conn.last_sync_at && <p>Last sync: {formatDate(conn.last_sync_at)}</p>}
                    </div>

                    {/* Actions */}
                    <div className="flex items-center gap-1 flex-shrink-0">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => goToSchema(conn.id)}
                      >
                        <ChevronRight className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => goToForm(conn)}
                      >
                        Edit
                      </Button>
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Delete Connection</AlertDialogTitle>
                            <AlertDialogDescription>
                              Are you sure you want to delete "{conn.name}"? This will remove the stored credentials. Datasets already extracted will not be affected.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => handleDelete(conn.id)}
                              className="bg-destructive hover:bg-destructive/90"
                            >
                              Delete
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    );
  }

  // =======================================================================
  // RENDER: Connection Form
  // =======================================================================

  if (view === "form") {
    return (
      <div className="space-y-6 animate-in fade-in duration-300 max-w-2xl">
        {/* Back */}
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2 text-muted-foreground hover:text-foreground"
          onClick={goToList}
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Connections
        </Button>

        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle>{editingId ? "Edit Connection" : "New Database Connection"}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            {/* Name */}
            <div className="space-y-2">
              <Label htmlFor="conn-name">Connection Name</Label>
              <Input
                id="conn-name"
                placeholder="e.g. Production Analytics DB"
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                className="bg-background border-border"
              />
            </div>

            {/* Type */}
            <div className="space-y-2">
              <Label>Database Type</Label>
              <Select value={form.db_type} onValueChange={(v) => handleTypeChange(v as "postgresql" | "mysql")}>
                <SelectTrigger className="bg-background border-border">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="postgresql">PostgreSQL</SelectItem>
                  <SelectItem value="mysql">MySQL</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Host + Port */}
            <div className="grid grid-cols-3 gap-4">
              <div className="col-span-2 space-y-2">
                <Label htmlFor="conn-host">Host</Label>
                <Input
                  id="conn-host"
                  placeholder="localhost or host.docker.internal"
                  value={form.host}
                  onChange={(e) => setForm((f) => ({ ...f, host: e.target.value }))}
                  className="bg-background border-border"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="conn-port">Port</Label>
                <Input
                  id="conn-port"
                  type="number"
                  value={form.port}
                  onChange={(e) => setForm((f) => ({ ...f, port: e.target.value }))}
                  className="bg-background border-border"
                />
              </div>
            </div>

            {/* Database */}
            <div className="space-y-2">
              <Label htmlFor="conn-db">Database</Label>
              <Input
                id="conn-db"
                placeholder="my_database"
                value={form.database}
                onChange={(e) => setForm((f) => ({ ...f, database: e.target.value }))}
                className="bg-background border-border"
              />
            </div>

            {/* Username */}
            <div className="space-y-2">
              <Label htmlFor="conn-user">Username</Label>
              <Input
                id="conn-user"
                placeholder="readonly_user"
                value={form.username}
                onChange={(e) => setForm((f) => ({ ...f, username: e.target.value }))}
                className="bg-background border-border"
              />
            </div>

            {/* Password */}
            <div className="space-y-2">
              <Label htmlFor="conn-pass">
                Password
                {editingId && (
                  <span className="text-xs text-muted-foreground ml-2">
                    (leave blank to keep current)
                  </span>
                )}
              </Label>
              <div className="relative">
                <Input
                  id="conn-pass"
                  type={showPassword ? "text" : "password"}
                  placeholder={editingId ? "****" : "Enter password"}
                  value={form.password}
                  onChange={(e) => setForm((f) => ({ ...f, password: e.target.value }))}
                  className="bg-background border-border pr-10"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((v) => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            {/* SSL Mode */}
            <div className="space-y-2">
              <Label>SSL Mode</Label>
              <Select value={form.ssl_mode} onValueChange={(v) => setForm((f) => ({ ...f, ssl_mode: v as FormState["ssl_mode"] }))}>
                <SelectTrigger className="bg-background border-border">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="disable">Disable</SelectItem>
                  <SelectItem value="prefer">Prefer (default)</SelectItem>
                  <SelectItem value="require">Require</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Test Result */}
            {testResult && (
              <div
                className={cn(
                  "flex items-center gap-3 p-3 rounded-lg border",
                  testResult.ok
                    ? "bg-haven-success/10 border-haven-success/30"
                    : "bg-destructive/10 border-destructive/30"
                )}
              >
                {testResult.ok ? (
                  <CheckCircle className="w-5 h-5 text-haven-success flex-shrink-0" />
                ) : (
                  <XCircle className="w-5 h-5 text-destructive flex-shrink-0" />
                )}
                <div className="text-sm">
                  {testResult.ok ? (
                    <>
                      <span className="font-medium text-haven-success">Connected</span>
                      <span className="text-muted-foreground ml-2">
                        {testResult.latency_ms}ms
                        {testResult.server_version && ` \u00b7 ${testResult.server_version}`}
                      </span>
                    </>
                  ) : (
                    <span className="text-destructive">{testResult.error}</span>
                  )}
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex items-center gap-3 pt-2">
              <Button variant="outline" onClick={handleTest} disabled={testing} className="gap-2">
                {testing ? <Loader2 className="w-4 h-4 animate-spin" /> : <Cable className="w-4 h-4" />}
                Test Connection
              </Button>
              <Button onClick={handleSave} disabled={saving} className="gap-2">
                {saving && <Loader2 className="w-4 h-4 animate-spin" />}
                {editingId ? "Update" : "Save"}
              </Button>
              <Button variant="ghost" onClick={goToList}>
                Cancel
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  // =======================================================================
  // RENDER: Schema Browser
  // =======================================================================

  return (
    <div className="space-y-6 animate-in fade-in duration-300">
      {/* Back + Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="sm"
            className="-ml-2 text-muted-foreground hover:text-foreground"
            onClick={goToList}
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back
          </Button>
          {selectedConnection && (
            <div className="flex items-center gap-2">
              <Server className="w-4 h-4 text-primary" />
              <span className="font-medium text-foreground">{selectedConnection.name}</span>
              <span className="text-sm text-muted-foreground">
                {selectedConnection.db_type === "postgresql" ? "PostgreSQL" : "MySQL"} &middot;{" "}
                {selectedConnection.host}:{selectedConnection.port}/{selectedConnection.database}
              </span>
            </div>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => selectedConnectionId && goToSchema(selectedConnectionId)}
            disabled={schemaLoading}
            className="gap-2"
          >
            <RefreshCw className={cn("w-4 h-4", schemaLoading && "animate-spin")} />
            Refresh
          </Button>
          {selectedConnectionId && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                const conn = connections.find((c) => c.id === selectedConnectionId);
                if (conn) goToForm(conn);
              }}
            >
              Edit
            </Button>
          )}
        </div>
      </div>

      {/* Loading */}
      {schemaLoading && (
        <div className="space-y-3">
          {[1, 2, 3, 4, 5].map((i) => (
            <Skeleton key={i} className="h-12 rounded-lg" />
          ))}
        </div>
      )}

      {/* Error */}
      {schemaError && (
        <Card className="border-destructive/50 bg-destructive/5">
          <CardContent className="py-4 flex items-center gap-3">
            <AlertCircle className="w-5 h-5 text-destructive flex-shrink-0" />
            <p className="text-sm text-destructive">{schemaError}</p>
            <Button
              variant="link"
              size="sm"
              className="ml-auto"
              onClick={() => selectedConnectionId && goToSchema(selectedConnectionId)}
            >
              Retry
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Empty schema */}
      {!schemaLoading && !schemaError && tables.length === 0 && (
        <Card className="bg-card border-border border-dashed">
          <CardContent className="py-12 text-center">
            <Database className="w-8 h-8 text-muted-foreground mx-auto mb-3" />
            <p className="text-muted-foreground">No tables found in this database.</p>
          </CardContent>
        </Card>
      )}

      {/* Tables */}
      {!schemaLoading && tables.length > 0 && (
        <>
          <Card className="bg-card border-border overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent border-border">
                  <TableHead className="w-10" />
                  <TableHead>Table</TableHead>
                  <TableHead>Schema</TableHead>
                  <TableHead>Columns</TableHead>
                  <TableHead>Est. Rows</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {tables.map((table) => {
                  const isExpanded = expandedTables.has(table.name);
                  const isSelected = selectedTables.has(table.name);
                  return (
                    <Collapsible key={`${table.schema}.${table.name}`} asChild open={isExpanded}>
                      <>
                        <TableRow className="border-border hover:bg-secondary/50">
                          <TableCell>
                            <Checkbox
                              checked={isSelected}
                              onCheckedChange={() => toggleTableSelect(table.name)}
                            />
                          </TableCell>
                          <TableCell className="font-mono text-sm font-medium">
                            {table.name}
                          </TableCell>
                          <TableCell className="text-muted-foreground text-sm">
                            {table.schema}
                          </TableCell>
                          <TableCell>{table.columns.length}</TableCell>
                          <TableCell>{formatNumber(table.estimated_rows)}</TableCell>
                          <TableCell>
                            <CollapsibleTrigger asChild>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => toggleTableExpand(table.name)}
                              >
                                {isExpanded ? (
                                  <ChevronDown className="w-4 h-4" />
                                ) : (
                                  <ChevronRight className="w-4 h-4" />
                                )}
                              </Button>
                            </CollapsibleTrigger>
                          </TableCell>
                        </TableRow>
                        <CollapsibleContent asChild>
                          <tr>
                            <td colSpan={6} className="p-0">
                              <div className="bg-secondary/30 px-8 py-3 border-b border-border">
                                <div className="grid grid-cols-3 gap-2 text-xs font-medium text-muted-foreground mb-2">
                                  <span>Column</span>
                                  <span>Type</span>
                                  <span>Nullable</span>
                                </div>
                                {table.columns.map((col) => (
                                  <div
                                    key={col.name}
                                    className="grid grid-cols-3 gap-2 text-sm py-1 border-t border-border/50"
                                  >
                                    <span className="font-mono text-foreground">{col.name}</span>
                                    <span className="text-muted-foreground">{col.type}</span>
                                    <span className="text-muted-foreground">{col.nullable ? "Yes" : "No"}</span>
                                  </div>
                                ))}
                              </div>
                            </td>
                          </tr>
                        </CollapsibleContent>
                      </>
                    </Collapsible>
                  );
                })}
              </TableBody>
            </Table>
          </Card>

          {/* Advanced: Custom SQL */}
          <Collapsible open={showCustomSql} onOpenChange={setShowCustomSql}>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm" className="gap-2 text-muted-foreground">
                {showCustomSql ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                Advanced: Custom SQL
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <Card className="bg-card border-border mt-2">
                <CardContent className="py-4 space-y-3">
                  <div className="space-y-2">
                    <Label htmlFor="sql-name">Dataset Name</Label>
                    <Input
                      id="sql-name"
                      placeholder="e.g. User Order Summary"
                      value={customSqlName}
                      onChange={(e) => setCustomSqlName(e.target.value)}
                      className="bg-background border-border"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="custom-sql">SQL Query (SELECT only)</Label>
                    <Textarea
                      id="custom-sql"
                      placeholder="SELECT u.name, COUNT(o.id) as order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
                      value={customSql}
                      onChange={(e) => setCustomSql(e.target.value)}
                      className="bg-background border-border font-mono text-sm min-h-[120px]"
                    />
                  </div>
                </CardContent>
              </Card>
            </CollapsibleContent>
          </Collapsible>

          {/* Extract Button */}
          <div className="flex items-center gap-4">
            <Button
              onClick={handleExtract}
              disabled={extracting || (selectedTables.size === 0 && !(showCustomSql && customSql.trim()))}
              className="gap-2"
            >
              {extracting ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Database className="w-4 h-4" />
              )}
              Extract {selectedTables.size > 0 ? `${selectedTables.size} Table${selectedTables.size > 1 ? "s" : ""}` : "Selected"}
            </Button>
            {selectedTables.size > 0 && (
              <span className="text-sm text-muted-foreground">
                {selectedTables.size} table{selectedTables.size !== 1 ? "s" : ""} selected
              </span>
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default DatabasePage;
