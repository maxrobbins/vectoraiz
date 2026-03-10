import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Plus, Clock, Loader2, AlertCircle, FileQuestion } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { fetchMyDataRequests, type DataRequest } from "@/lib/data-requests-api";
import { useMode } from "@/contexts/ModeContext";

const statusColors: Record<string, string> = {
  draft: "bg-muted text-muted-foreground",
  open: "bg-green-500/10 text-green-500 border-green-500/20",
  responses_received: "bg-blue-500/10 text-blue-500 border-blue-500/20",
  fulfilled: "bg-primary/10 text-primary border-primary/20",
  closed: "bg-muted text-muted-foreground",
  expired: "bg-muted text-muted-foreground",
};

const DashboardRequestsPage = () => {
  const navigate = useNavigate();
  const { isLoading: modeLoading, isConnected } = useMode();
  const [requests, setRequests] = useState<DataRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (modeLoading) return;
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetchMyDataRequests();
        setRequests(res.items || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load your requests");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [modeLoading, isConnected]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <h2 className="text-3xl font-bold text-foreground">My Data Requests</h2>
          <p className="text-muted-foreground">
            Track your posted data requests and incoming responses.
          </p>
        </div>
        <Button className="gap-2" onClick={() => navigate("/data-requests/new")}>
          <Plus className="w-4 h-4" />
          New Request
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <Card className="bg-card border-border">
          <CardContent className="py-12">
            <div className="flex flex-col items-center justify-center text-center space-y-3">
              <AlertCircle className="w-8 h-8 text-destructive" />
              <p className="text-muted-foreground">{error}</p>
            </div>
          </CardContent>
        </Card>
      ) : requests.length === 0 ? (
        <Card className="bg-card border-border">
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-14 h-14 rounded-full bg-secondary flex items-center justify-center">
                <FileQuestion className="w-7 h-7 text-muted-foreground" />
              </div>
              <div className="space-y-1">
                <p className="text-foreground font-medium">No data requests yet</p>
                <p className="text-sm text-muted-foreground">
                  Post a request to tell sellers what data you need.
                </p>
              </div>
              <Button className="gap-2" onClick={() => navigate("/data-requests/new")}>
                <Plus className="w-4 h-4" />
                Post a Data Request
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {requests.map((req) => (
            <Link key={req.id} to={`/data-requests/${req.slug}`}>
              <Card className="bg-card border-border card-hover cursor-pointer">
                <CardContent className="py-4">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex-1 min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <h3 className="text-foreground font-semibold truncate">
                          {req.title}
                        </h3>
                        <Badge variant="outline" className={statusColors[req.status]}>
                          {req.status.replace("_", " ")}
                        </Badge>
                      </div>
                      <div className="flex items-center gap-3 text-xs text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {new Date(req.created_at).toLocaleDateString()}
                        </span>
                        <span>
                          {req.response_count} response{req.response_count !== 1 ? "s" : ""}
                        </span>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
};

export default DashboardRequestsPage;
