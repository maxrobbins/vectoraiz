import { useLocation } from "react-router-dom";
import { User, Search, Command, Wifi, WifiOff, Loader2, LogOut, Settings } from "lucide-react";
import NotificationBell from "@/components/notifications/NotificationBell";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useBackendConnection } from "@/hooks/useApi";
import { useAuth } from "@/contexts/AuthContext";
import { useNavigate } from "react-router-dom";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const pageTitles: Record<string, string> = {
  "/": "Dashboard",
  "/datasets": "Datasets",
  "/earnings": "Earnings",
  "/search": "Search",
  "/sql": "SQL Query",
  "/settings": "Settings",
  "/data-types": "Data Types",
  "/ai-market": "ai.market",
};

interface TopBarProps {
  onOpenCommandPalette?: () => void;
}

const TopBar = ({ onOpenCommandPalette }: TopBarProps) => {
  const location = useLocation();
  const navigate = useNavigate();
  const title = pageTitles[location.pathname] || "vectorAIz";
  const { status } = useBackendConnection();
  const { logout } = useAuth();

  return (
    <header className="h-16 bg-card border-b border-border flex items-center justify-between px-6">
      <h1 className="text-xl font-semibold text-foreground">{title}</h1>
      
      <div className="flex items-center gap-3">
        {/* Search button */}
        <Button
          variant="outline"
          size="sm"
          className="gap-2 text-muted-foreground"
          onClick={onOpenCommandPalette}
        >
          <Search className="w-4 h-4" />
          <span className="hidden sm:inline">Search...</span>
          <kbd className="hidden sm:inline-flex pointer-events-none h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
            <Command className="h-3 w-3" />K
          </kbd>
        </Button>

        {/* Notifications */}
        <NotificationBell />

        {/* Connection Status */}
        <Tooltip>
          <TooltipTrigger asChild>
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-secondary border border-border">
              {status === 'checking' && (
                <>
                  <Loader2 className="w-3 h-3 animate-spin text-muted-foreground" />
                  <span className="text-xs font-medium text-muted-foreground">
                    Checking...
                  </span>
                </>
              )}
              {status === 'connected' && (
                <>
                  <Wifi className="w-3 h-3 text-[hsl(var(--haven-success))]" />
                  <span className="text-xs font-medium text-[hsl(var(--haven-success))]">
                    Connected
                  </span>
                </>
              )}
              {status === 'disconnected' && (
                <>
                  <WifiOff className="w-3 h-3 text-destructive" />
                  <span className="text-xs font-medium text-destructive">
                    Disconnected
                  </span>
                </>
              )}
            </div>
          </TooltipTrigger>
          <TooltipContent>
            {status === 'connected'
              ? "Connected to vectorAIz backend"
              : status === 'disconnected'
                ? "Backend not reachable. Check Settings to configure."
                : "Checking backend connection..."}
          </TooltipContent>
        </Tooltip>

        {/* User menu */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="w-9 h-9 rounded-full bg-secondary flex items-center justify-center border border-border hover:bg-accent transition-colors cursor-pointer">
              <User className="w-5 h-5 text-muted-foreground" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-48">
            <DropdownMenuItem onClick={() => navigate("/settings")} className="cursor-pointer">
              <Settings className="w-4 h-4 mr-2" />
              Settings
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={logout} className="cursor-pointer text-destructive focus:text-destructive">
              <LogOut className="w-4 h-4 mr-2" />
              Sign Out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
};

export default TopBar;