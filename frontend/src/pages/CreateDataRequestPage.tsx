import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft, Loader2, X } from "lucide-react";
import { useMode } from "@/contexts/ModeContext";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { createDataRequest, type Urgency } from "@/lib/data-requests-api";

const CATEGORIES = [
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

function autoTitle(desc: string): string {
  const trimmed = desc.trim();
  if (trimmed.length <= 60) return trimmed;
  const cut = trimmed.slice(0, 60);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > 0 ? cut.slice(0, lastSpace) : cut) + "...";
}

const CreateDataRequestPage = () => {
  const navigate = useNavigate();
  const { isLoading: modeLoading } = useMode();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState("");

  const [description, setDescription] = useState("");
  const [categories, setCategories] = useState<string[]>([]);
  const [categoryInput, setCategoryInput] = useState("");
  const [formatPreferences, setFormatPreferences] = useState("");
  const [priceMin, setPriceMin] = useState("");
  const [priceMax, setPriceMax] = useState("");
  const [currency, setCurrency] = useState("USD");
  const [urgency, setUrgency] = useState<Urgency>("medium");
  const [provenanceRequirements, setProvenanceRequirements] = useState("");

  const addCategory = (cat: string) => {
    if (cat && !categories.includes(cat)) {
      setCategories([...categories, cat]);
    }
    setCategoryInput("");
  };

  const removeCategory = (cat: string) => {
    setCategories(categories.filter((c) => c !== cat));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!description.trim()) {
      setError("Description is required");
      return;
    }

    setIsSubmitting(true);
    try {
      const result = await createDataRequest({
        title: autoTitle(description),
        description: description.trim(),
        categories,
        format_preferences: formatPreferences.trim() || undefined,
        price_range_min: priceMin ? Number(priceMin) : undefined,
        price_range_max: priceMax ? Number(priceMax) : undefined,
        currency,
        urgency,
        provenance_requirements: provenanceRequirements.trim() || undefined,
      });
      toast.success("Data request created as draft");
      navigate(`/data-requests/${result.slug}`);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to create request");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="max-w-2xl space-y-6">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="icon" onClick={() => navigate("/data-requests")}>
          <ArrowLeft className="w-5 h-5" />
        </Button>
        <div>
          <h2 className="text-2xl font-bold text-foreground">Post a Data Request</h2>
          <p className="text-sm text-muted-foreground">
            Describe what data you need — sellers will respond with proposals
          </p>
        </div>
      </div>

      {modeLoading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
        </div>
      ) : (
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <form onSubmit={handleSubmit} className="space-y-5">
            {error && (
              <div className="p-3 text-sm text-destructive bg-destructive/10 border border-destructive/20 rounded-lg">
                {error}
              </div>
            )}

            <div className="space-y-2">
              <Label htmlFor="description">Description *</Label>
              <Textarea
                id="description"
                placeholder="Describe the data you need: scope, granularity, time range, geography, etc."
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={5}
              />
            </div>

            <div className="space-y-2">
              <Label>Categories</Label>
              <div className="flex flex-wrap gap-1.5 mb-2">
                {categories.map((cat) => (
                  <Badge key={cat} variant="secondary" className="gap-1">
                    {cat}
                    <button
                      type="button"
                      onClick={() => removeCategory(cat)}
                      className="hover:text-destructive"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </Badge>
                ))}
              </div>
              <Select value={categoryInput} onValueChange={(v) => addCategory(v)}>
                <SelectTrigger>
                  <SelectValue placeholder="Add a category" />
                </SelectTrigger>
                <SelectContent>
                  {CATEGORIES.filter((c) => !categories.includes(c)).map((c) => (
                    <SelectItem key={c} value={c}>
                      {c}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="format">Format Preferences</Label>
              <Input
                id="format"
                placeholder="e.g. CSV, JSON, Parquet"
                value={formatPreferences}
                onChange={(e) => setFormatPreferences(e.target.value)}
              />
            </div>

            <div className="grid grid-cols-3 gap-3">
              <div className="space-y-2">
                <Label htmlFor="priceMin">Price Min</Label>
                <Input
                  id="priceMin"
                  type="number"
                  min={0}
                  placeholder="0"
                  value={priceMin}
                  onChange={(e) => setPriceMin(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="priceMax">Price Max</Label>
                <Input
                  id="priceMax"
                  type="number"
                  min={0}
                  placeholder="10000"
                  value={priceMax}
                  onChange={(e) => setPriceMax(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="currency">Currency</Label>
                <Select value={currency} onValueChange={setCurrency}>
                  <SelectTrigger id="currency">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="USD">USD</SelectItem>
                    <SelectItem value="EUR">EUR</SelectItem>
                    <SelectItem value="GBP">GBP</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="urgency">Urgency</Label>
              <Select value={urgency} onValueChange={(v) => setUrgency(v as Urgency)}>
                <SelectTrigger id="urgency">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="low">Low</SelectItem>
                  <SelectItem value="medium">Medium</SelectItem>
                  <SelectItem value="high">High</SelectItem>
                  <SelectItem value="urgent">Urgent</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="provenance">Provenance Requirements</Label>
              <Textarea
                id="provenance"
                placeholder="Any requirements for data sourcing, licensing, or provenance documentation..."
                value={provenanceRequirements}
                onChange={(e) => setProvenanceRequirements(e.target.value)}
                rows={3}
              />
            </div>

            <div className="flex items-center gap-3 pt-2">
              <Button type="submit" disabled={isSubmitting} className="gap-2">
                {isSubmitting && <Loader2 className="w-4 h-4 animate-spin" />}
                Create Draft
              </Button>
              <Button type="button" variant="outline" onClick={() => navigate("/data-requests")}>
                Cancel
              </Button>
            </div>

            <p className="text-xs text-muted-foreground">
              Your request will be created as a draft. You can publish it from the detail page.
            </p>
          </form>
        </CardContent>
      </Card>
      )}
    </div>
  );
};

export default CreateDataRequestPage;
