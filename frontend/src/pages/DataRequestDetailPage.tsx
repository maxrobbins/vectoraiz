import { useEffect, useState } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import {
  ArrowLeft,
  Clock,
  Tag,
  Send,
  Loader2,
  CheckCircle,
  XCircle,
  AlertCircle,
  Trash2,
  Globe,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import { useAuth } from "@/contexts/AuthContext";
import { useMode } from "@/contexts/ModeContext";
import {
  fetchDataRequest,
  fetchResponses,
  publishDataRequest,
  deleteDataRequest,
  submitResponse,
  updateResponse,
  type DataRequest,
  type RequestResponse,
  type Urgency,
} from "@/lib/data-requests-api";

const urgencyColors: Record<Urgency, string> = {
  low: "bg-muted text-muted-foreground",
  medium: "bg-blue-500/10 text-blue-500 border-blue-500/20",
  high: "bg-amber-500/10 text-amber-500 border-amber-500/20",
  urgent: "bg-red-500/10 text-red-500 border-red-500/20",
};

const statusColors: Record<string, string> = {
  draft: "bg-muted text-muted-foreground",
  open: "bg-green-500/10 text-green-500 border-green-500/20",
  responses_received: "bg-blue-500/10 text-blue-500 border-blue-500/20",
  fulfilled: "bg-primary/10 text-primary border-primary/20",
  closed: "bg-muted text-muted-foreground",
  expired: "bg-muted text-muted-foreground",
};

const responseStatusColors: Record<string, string> = {
  pending: "bg-amber-500/10 text-amber-500 border-amber-500/20",
  accepted: "bg-green-500/10 text-green-500 border-green-500/20",
  rejected: "bg-red-500/10 text-red-500 border-red-500/20",
  withdrawn: "bg-muted text-muted-foreground",
};

function formatPrice(min?: number, max?: number, currency = "USD"): string {
  if (!min && !max) return "Open budget";
  const fmt = (n: number) =>
    new Intl.NumberFormat("en-US", { style: "currency", currency, maximumFractionDigits: 0 }).format(n);
  if (min && max) return `${fmt(min)} – ${fmt(max)}`;
  if (min) return `From ${fmt(min)}`;
  return `Up to ${fmt(max!)}`;
}

const DataRequestDetailPage = () => {
  const { slug } = useParams<{ slug: string }>();
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();
  const { isLoading: modeLoading, isConnected } = useMode();

  const [request, setRequest] = useState<DataRequest | null>(null);
  const [responses, setResponses] = useState<RequestResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Response form state
  const [proposal, setProposal] = useState("");
  const [proposedPrice, setProposedPrice] = useState("");
  const [timeline, setTimeline] = useState("");
  const [submittingResponse, setSubmittingResponse] = useState(false);

  const [publishing, setPublishing] = useState(false);
  const [deleting, setDeleting] = useState(false);

  useEffect(() => {
    if (!slug || modeLoading) return;
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const req = await fetchDataRequest(slug);
        setRequest(req);
        if (req.is_owner || isAuthenticated) {
          try {
            const resps = await fetchResponses(req.id);
            setResponses(resps);
          } catch {
            // Responses may not be accessible for non-authenticated users
          }
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load request");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [slug, isAuthenticated, modeLoading, isConnected]);

  const handlePublish = async () => {
    if (!request) return;
    setPublishing(true);
    try {
      const updated = await publishDataRequest(request.id);
      setRequest(updated);
      toast.success("Request published — now visible to sellers");
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to publish");
    } finally {
      setPublishing(false);
    }
  };

  const handleDelete = async () => {
    if (!request) return;
    setDeleting(true);
    try {
      await deleteDataRequest(request.id);
      toast.success("Request deleted");
      navigate("/data-requests");
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to delete");
    } finally {
      setDeleting(false);
    }
  };

  const handleSubmitResponse = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!request || !proposal.trim()) return;
    setSubmittingResponse(true);
    try {
      const resp = await submitResponse(request.id, {
        proposal: proposal.trim(),
        proposed_price: proposedPrice ? Number(proposedPrice) : undefined,
        timeline: timeline.trim() || undefined,
      });
      setResponses([...responses, resp]);
      setProposal("");
      setProposedPrice("");
      setTimeline("");
      toast.success("Response submitted");
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to submit response");
    } finally {
      setSubmittingResponse(false);
    }
  };

  const handleResponseAction = async (responseId: string, status: "accepted" | "rejected") => {
    try {
      const updated = await updateResponse(responseId, { status });
      setResponses(responses.map((r) => (r.id === responseId ? updated : r)));
      toast.success(`Response ${status}`);
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to update response");
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error || !request) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" size="icon" onClick={() => navigate("/data-requests")}>
          <ArrowLeft className="w-5 h-5" />
        </Button>
        <Card className="bg-card border-border">
          <CardContent className="py-12">
            <div className="flex flex-col items-center justify-center text-center space-y-3">
              <AlertCircle className="w-8 h-8 text-destructive" />
              <p className="text-muted-foreground">{error || "Request not found"}</p>
              <Button variant="outline" asChild>
                <Link to="/data-requests">Back to requests</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="max-w-3xl space-y-6">
      {/* Back + Actions */}
      <div className="flex items-center justify-between">
        <Button variant="ghost" size="icon" onClick={() => navigate("/data-requests")}>
          <ArrowLeft className="w-5 h-5" />
        </Button>
        {request.is_owner && (
          <div className="flex items-center gap-2">
            {request.status === "draft" && (
              <Button onClick={handlePublish} disabled={publishing} className="gap-2">
                {publishing ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Globe className="w-4 h-4" />
                )}
                Publish
              </Button>
            )}
            <Button
              variant="destructive"
              size="icon"
              onClick={handleDelete}
              disabled={deleting}
            >
              {deleting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
            </Button>
          </div>
        )}
      </div>

      {/* Main Detail Card */}
      <Card className="bg-card border-border">
        <CardHeader>
          <div className="space-y-3">
            <div className="flex items-center gap-2 flex-wrap">
              <Badge variant="outline" className={statusColors[request.status]}>
                {request.status.replace("_", " ")}
              </Badge>
              <Badge variant="outline" className={urgencyColors[request.urgency]}>
                {request.urgency}
              </Badge>
            </div>
            <CardTitle className="text-2xl">{request.title}</CardTitle>
            <div className="flex items-center gap-4 text-sm text-muted-foreground">
              {request.requester_pseudonym && (
                <span>by {request.requester_pseudonym}</span>
              )}
              <span className="flex items-center gap-1">
                <Clock className="w-3.5 h-3.5" />
                {new Date(request.published_at || request.created_at).toLocaleDateString()}
              </span>
              <span>
                {request.response_count} response{request.response_count !== 1 ? "s" : ""}
              </span>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-5">
          <p className="text-foreground whitespace-pre-wrap">{request.description}</p>

          <div className="grid grid-cols-2 gap-4 text-sm">
            {request.categories?.length > 0 && (
              <div>
                <span className="text-muted-foreground">Categories</span>
                <div className="flex gap-1 mt-1 flex-wrap">
                  {request.categories.map((cat) => (
                    <Badge key={cat} variant="secondary" className="text-xs">
                      <Tag className="w-3 h-3 mr-1" />
                      {cat}
                    </Badge>
                  ))}
                </div>
              </div>
            )}
            <div>
              <span className="text-muted-foreground">Budget</span>
              <p className="font-medium text-foreground mt-1">
                {formatPrice(request.price_range_min, request.price_range_max, request.currency)}
              </p>
            </div>
            {request.format_preferences && (
              <div>
                <span className="text-muted-foreground">Format</span>
                <p className="text-foreground mt-1">{request.format_preferences}</p>
              </div>
            )}
            {request.provenance_requirements && (
              <div className="col-span-2">
                <span className="text-muted-foreground">Provenance Requirements</span>
                <p className="text-foreground mt-1 whitespace-pre-wrap">
                  {request.provenance_requirements}
                </p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Responses Section (owner view) */}
      {request.is_owner && responses.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-lg">
              Responses ({responses.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {responses.map((resp) => (
              <div
                key={resp.id}
                className="p-4 border border-border rounded-lg space-y-3"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    {resp.responder_pseudonym && (
                      <span className="text-sm font-medium text-foreground">
                        {resp.responder_pseudonym}
                      </span>
                    )}
                    <Badge variant="outline" className={responseStatusColors[resp.status]}>
                      {resp.status}
                    </Badge>
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {new Date(resp.created_at).toLocaleDateString()}
                  </span>
                </div>
                <p className="text-sm text-foreground whitespace-pre-wrap">{resp.proposal}</p>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  {resp.proposed_price != null && (
                    <span>
                      Price:{" "}
                      {new Intl.NumberFormat("en-US", {
                        style: "currency",
                        currency: resp.currency || "USD",
                      }).format(resp.proposed_price)}
                    </span>
                  )}
                  {resp.timeline && <span>Timeline: {resp.timeline}</span>}
                </div>
                {resp.status === "pending" && request.is_owner && (
                  <div className="flex items-center gap-2 pt-1">
                    <Button
                      size="sm"
                      className="gap-1"
                      onClick={() => handleResponseAction(resp.id, "accepted")}
                    >
                      <CheckCircle className="w-3.5 h-3.5" />
                      Accept
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="gap-1"
                      onClick={() => handleResponseAction(resp.id, "rejected")}
                    >
                      <XCircle className="w-3.5 h-3.5" />
                      Reject
                    </Button>
                  </div>
                )}
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {/* Submit Response Form (seller view — logged in, not owner) */}
      {isAuthenticated && !request.is_owner && request.status !== "draft" && request.status !== "closed" && (
        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-lg">Submit a Response</CardTitle>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmitResponse} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="proposal">Your Proposal *</Label>
                <Textarea
                  id="proposal"
                  placeholder="Describe the data you can provide and how it meets this request..."
                  value={proposal}
                  onChange={(e) => setProposal(e.target.value)}
                  rows={4}
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="resp-price">Proposed Price</Label>
                  <Input
                    id="resp-price"
                    type="number"
                    min={0}
                    placeholder="1000"
                    value={proposedPrice}
                    onChange={(e) => setProposedPrice(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="resp-timeline">Timeline</Label>
                  <Input
                    id="resp-timeline"
                    placeholder="e.g. 2 weeks"
                    value={timeline}
                    onChange={(e) => setTimeline(e.target.value)}
                  />
                </div>
              </div>
              <Button type="submit" disabled={submittingResponse || !proposal.trim()} className="gap-2">
                {submittingResponse ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Send className="w-4 h-4" />
                )}
                Submit Response
              </Button>
            </form>
          </CardContent>
        </Card>
      )}

      {/* Login prompt for non-authenticated users */}
      {!isAuthenticated && request.status !== "draft" && (
        <Card className="bg-card border-border">
          <CardContent className="py-8">
            <div className="flex flex-col items-center text-center space-y-3">
              <p className="text-muted-foreground">
                Have data that matches this request?
              </p>
              <Button asChild>
                <Link to="/login">Log in to respond</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default DataRequestDetailPage;
