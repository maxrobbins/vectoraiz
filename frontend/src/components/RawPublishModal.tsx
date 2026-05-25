import { useState } from "react";
import { Loader2, Store, X } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { rawFilesApi, type RawFile } from "@/lib/api";

interface RawPublishModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  rawFile: RawFile;
  onPublishSuccess: () => void;
}

const RawPublishModal = ({
  open,
  onOpenChange,
  rawFile,
  onPublishSuccess,
}: RawPublishModalProps) => {
  const defaultTitle = rawFile.filename.replace(/\.[^/.]+$/, "").replace(/[-_]/g, " ");

  const [title, setTitle] = useState(defaultTitle);
  const [description, setDescription] = useState("");
  const [tagInput, setTagInput] = useState("");
  const [tags, setTags] = useState<string[]>([]);
  const [priceUsd, setPriceUsd] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const addTag = () => {
    const t = tagInput.trim();
    if (!t || tags.includes(t)) {
      setTagInput("");
      return;
    }
    if (tags.length >= 20) return;
    setTags([...tags, t]);
    setTagInput("");
  };

  const removeTag = (t: string) => setTags(tags.filter((x) => x !== t));

  const handleSubmit = async () => {
    setError(null);

    if (!title.trim()) {
      setError("Title is required.");
      return;
    }
    if (!description.trim()) {
      setError("Description is required.");
      return;
    }

    let priceCents: number | undefined = undefined;
    if (priceUsd.trim()) {
      const usd = parseFloat(priceUsd);
      if (isNaN(usd) || usd < 0) {
        setError("Price must be a non-negative number.");
        return;
      }
      priceCents = Math.round(usd * 100);
    }

    setSubmitting(true);
    try {
      const draft = await rawFilesApi.createRawListing({
        raw_file_id: rawFile.id,
        title: title.trim(),
        description: description.trim(),
        tags,
        price_cents: priceCents,
      });
      await rawFilesApi.publishRawListing(draft.id);
      onPublishSuccess();
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Publish failed";
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Store className="w-5 h-5" />
            Publish to ai.market
          </DialogTitle>
          <DialogDescription>
            Listing <span className="font-mono text-xs">{rawFile.filename}</span> on the marketplace.
            Buyers see your title, description, and tags before purchase.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="raw-title">Title</Label>
            <Input
              id="raw-title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              maxLength={256}
              placeholder="e.g. 2024 sales transactions"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="raw-desc">Description</Label>
            <Textarea
              id="raw-desc"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={4}
              placeholder="What this data is, who it's for, what's in it..."
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="raw-tags">Tags <span className="text-muted-foreground text-xs">(optional, press Enter to add)</span></Label>
            <div className="flex gap-2">
              <Input
                id="raw-tags"
                value={tagInput}
                onChange={(e) => setTagInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    addTag();
                  }
                }}
                placeholder="finance"
              />
              <Button type="button" variant="outline" onClick={addTag} disabled={!tagInput.trim()}>
                Add
              </Button>
            </div>
            {tags.length > 0 && (
              <div className="flex flex-wrap gap-1.5 pt-1">
                {tags.map((t) => (
                  <Badge key={t} variant="secondary" className="gap-1">
                    {t}
                    <button
                      type="button"
                      onClick={() => removeTag(t)}
                      className="hover:text-destructive"
                      aria-label={`Remove tag ${t}`}
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}
          </div>

          <div className="space-y-2">
            <Label htmlFor="raw-price">Price <span className="text-muted-foreground text-xs">(USD, optional — leave empty for free)</span></Label>
            <div className="relative">
              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground">$</span>
              <Input
                id="raw-price"
                type="number"
                min="0"
                step="0.01"
                value={priceUsd}
                onChange={(e) => setPriceUsd(e.target.value)}
                placeholder="0.00"
                className="pl-7"
              />
            </div>
          </div>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={() => onOpenChange(false)} disabled={submitting}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={submitting} className="gap-2">
            {submitting ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Publishing...
              </>
            ) : (
              <>
                <Store className="w-4 h-4" />
                Publish
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default RawPublishModal;
