import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Plus, Clock, Tag, AlertCircle, Loader2, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useAuth } from "@/contexts/AuthContext";
import { useMode } from "@/contexts/ModeContext";
import {
  fetchDataRequests,
  type DataRequest,
  type Urgency,
} from "@/lib/data-requests-api";

const urgencyColors: Record<Urgency, string> = {
  low: "bg-muted text-muted-foreground",
  medium: "bg-blue-500/10 text-blue-500 border-blue-500/20",
  high: "bg-amber-500/10 text-amber-500 border-amber-500/20",
  urgent: "bg-red-500/10 text-red-500 border-red-500/20",
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const minutes = Math.floor(diff / 60000);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
}

function formatPrice(min?: number, max?: number, currency = "USD"): string {
  if (!min && !max) return "Open";
  const fmt = (n: number) =>
    new Intl.NumberFormat("en-US", { style: "currency", currency, maximumFractionDigits: 0 }).format(n);
  if (min && max) return `${fmt(min)} – ${fmt(max)}`;
  if (min) return `From ${fmt(min)}`;
  return `Up to ${fmt(max!)}`;
}

const CATEGORIES = [
  "All Categories",
  "Finance",
  "Healthcare",
  "Technology",
  "Energy",
  "Real Estate",
  "Government",
  "Education",
  "Retail",
  "Other",
];

const DataRequestsPage = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();
  const { isLoading: modeLoading, isConnected } = useMode();
  const [requests, setRequests] = useState<DataRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [category, setCategory] = useState("All Categories");
  const [sort, setSort] = useState("newest");

  useEffect(() => {
    if (modeLoading) return;
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const params: Record<string, string> = { sort };
        if (category !== "All Categories") params.category = category;
        const res = await fetchDataRequests(params);
        setRequests(res.items || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data requests");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [category, sort, modeLoading, isConnected]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <h2 className="text-3xl font-bold text-foreground">Data Requests</h2>
          <p className="text-muted-foreground">
            Browse what buyers need — or post your own request.
          </p>
        </div>
        <Button
          className="gap-2"
          onClick={() => {
            if (isAuthenticated) {
              navigate("/data-requests/new");
            } else {
              navigate("/login");
            }
          }}
        >
          <Plus className="w-4 h-4" />
          Post a Data Request
        </Button>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3">
        <Select value={category} onValueChange={setCategory}>
          <SelectTrigger className="w-[180px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {CATEGORIES.map((c) => (
              <SelectItem key={c} value={c}>
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={sort} onValueChange={setSort}>
          <SelectTrigger className="w-[150px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="newest">Newest</SelectItem>
            <SelectItem value="oldest">Oldest</SelectItem>
            <SelectItem value="urgency">Most Urgent</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Content */}
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
              <Button variant="outline" onClick={() => window.location.reload()}>
                Retry
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : requests.length === 0 ? (
        <Card className="bg-card border-border">
          <CardContent className="py-16">
            <div className="flex flex-col items-center justify-center text-center space-y-4">
              <div className="w-14 h-14 rounded-full bg-secondary flex items-center justify-center">
                <Search className="w-7 h-7 text-muted-foreground" />
              </div>
              <div className="space-y-1">
                <p className="text-foreground font-medium">No data requests yet</p>
                <p className="text-sm text-muted-foreground">
                  Be the first to tell the market what you need.
                </p>
              </div>
              <Button
                className="gap-2"
                onClick={() => navigate(isAuthenticated ? "/data-requests/new" : "/login")}
              >
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
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 min-w-0 space-y-2">
                      <div className="flex items-center gap-2">
                        <h3 className="text-foreground font-semibold truncate">
                          {req.title}
                        </h3>
                        <Badge
                          variant="outline"
                          className={urgencyColors[req.urgency]}
                        >
                          {req.urgency}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground line-clamp-2">
                        {req.description}
                      </p>
                      <div className="flex items-center gap-4 text-xs text-muted-foreground">
                        {req.categories?.length > 0 && (
                          <span className="flex items-center gap-1">
                            <Tag className="w-3 h-3" />
                            {req.categories.join(", ")}
                          </span>
                        )}
                        <span className="flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {timeAgo(req.published_at || req.created_at)}
                        </span>
                        {req.response_count > 0 && (
                          <span>
                            {req.response_count} response{req.response_count !== 1 ? "s" : ""}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="text-right shrink-0">
                      <span className="text-sm font-medium text-foreground">
                        {formatPrice(req.price_range_min, req.price_range_max, req.currency)}
                      </span>
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

export default DataRequestsPage;
