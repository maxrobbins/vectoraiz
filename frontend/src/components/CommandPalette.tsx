import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  LayoutDashboard,
  Database,
  Search,
  Code,
  Settings,
  DollarSign,
  Upload,
  FileSpreadsheet,
  Loader2,
  FileOutput,
  Store,
  HandHelping,
  FileStack,
} from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { useDatasets } from "@/hooks/useApi";

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onOpenUpload?: () => void;
}

const pages = [
  { name: "Dashboard", path: "/", icon: LayoutDashboard },
  { name: "Datasets", path: "/datasets", icon: Database },
  { name: "Search", path: "/search", icon: Search },
  { name: "SQL Query", path: "/sql", icon: Code },
  { name: "Artifacts", path: "/artifacts", icon: FileOutput },
  { name: "Databases", path: "/databases", icon: Database },
  { name: "Settings", path: "/settings", icon: Settings },
  { name: "ai.market", path: "/ai-market", icon: Store },
  { name: "I Need Data", path: "/data-requests", icon: HandHelping },
  { name: "Earnings", path: "/earnings", icon: DollarSign },
  { name: "Data Types", path: "/data-types", icon: FileStack },
];

const CommandPalette = ({ open, onOpenChange, onOpenUpload }: CommandPaletteProps) => {
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  
  // Fetch datasets from API
  const { data: datasetsData, loading: datasetsLoading } = useDatasets();
  const datasets = datasetsData?.datasets || [];

  // Filter datasets based on search
  const filteredDatasets = datasets.filter((dataset) =>
    dataset.original_filename.toLowerCase().includes(search.toLowerCase())
  );

  const handleSelect = (path: string) => {
    navigate(path);
    onOpenChange(false);
  };

  const handleUpload = () => {
    onOpenChange(false);
    onOpenUpload?.();
  };

  // Reset search when dialog closes
  useEffect(() => {
    if (!open) {
      setSearch("");
    }
  }, [open]);

  return (
    <CommandDialog open={open} onOpenChange={onOpenChange}>
      <CommandInput
        placeholder="Search datasets, pages, or actions..."
        value={search}
        onValueChange={setSearch}
      />
      <CommandList>
        <CommandEmpty>No results found.</CommandEmpty>

        {/* Quick Actions */}
        <CommandGroup heading="Quick Actions">
          <CommandItem onSelect={handleUpload}>
            <Upload className="mr-2 h-4 w-4" />
            <span>Upload new dataset</span>
            <kbd className="ml-auto pointer-events-none inline-flex h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
              <span className="text-xs">⌘</span>U
            </kbd>
          </CommandItem>
        </CommandGroup>

        <CommandSeparator />

        {/* Pages */}
        <CommandGroup heading="Pages">
          {pages.map((page) => (
            <CommandItem
              key={page.path}
              onSelect={() => handleSelect(page.path)}
            >
              <page.icon className="mr-2 h-4 w-4" />
              <span>{page.name}</span>
            </CommandItem>
          ))}
        </CommandGroup>

        <CommandSeparator />

        {/* Datasets */}
        <CommandGroup heading="Datasets">
          {datasetsLoading ? (
            <CommandItem disabled>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              <span>Loading datasets...</span>
            </CommandItem>
          ) : filteredDatasets.length > 0 ? (
            filteredDatasets.slice(0, 5).map((dataset) => (
              <CommandItem
                key={dataset.id}
                onSelect={() => handleSelect(`/datasets/${dataset.id}`)}
              >
                <FileSpreadsheet className="mr-2 h-4 w-4" />
                <span>{dataset.original_filename}</span>
                <span className="ml-auto text-xs text-muted-foreground">
                  {(dataset.metadata?.row_count || 0).toLocaleString()} rows
                </span>
              </CommandItem>
            ))
          ) : (
            <CommandItem disabled>
              <span className="text-muted-foreground">No datasets found</span>
            </CommandItem>
          )}
        </CommandGroup>
      </CommandList>
    </CommandDialog>
  );
};

export default CommandPalette;
