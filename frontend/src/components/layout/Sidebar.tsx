import { NavLink, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  Database,
  Search,
  Code,
  Settings,
  ChevronLeft,
  ChevronRight,
  DollarSign,
  Monitor,
  Globe,
  FileStack,
  FileOutput,
  Store,
  HandHelping,
  CreditCard,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useMode } from "@/contexts/ModeContext";
import { useBrand } from "@/contexts/BrandContext";
import { useChannel } from "@/hooks/useChannel";

interface NavItem {
  path: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  feature?: "marketplace";
}

// All nav items — both channels contain identical items, just reordered (CH-C2)
const ALL_NAV_ITEMS: NavItem[] = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/datasets", label: "Datasets", icon: Database },
  { path: "/search", label: "Search", icon: Search },
  { path: "/sql", label: "SQL Query", icon: Code },
  { path: "/artifacts", label: "Artifacts", icon: FileOutput },
  { path: "/databases", label: "Databases", icon: Database },
  { path: "/settings", label: "Settings", icon: Settings },
  { path: "/billing", label: "Billing", icon: CreditCard },
  { path: "/ai-market", label: "ai.market", icon: Store },
  { path: "/data-requests", label: "I Need Data", icon: HandHelping },
  { path: "/earnings", label: "Earnings", icon: DollarSign, feature: "marketplace" },
  { path: "/data-types", label: "Data Types", icon: FileStack },
];

// direct channel: data-focused items first, marketplace items at bottom
const NAV_ORDER_DIRECT = [
  "/", "/datasets", "/search", "/sql", "/artifacts", "/databases", "/settings",
  "/ai-market", "/data-requests", "/earnings", "/billing", "/data-types",
];

// marketplace channel: marketplace items promoted to top, then data items
const NAV_ORDER_MARKETPLACE = [
  "/ai-market", "/data-requests", "/",
  "/datasets", "/search", "/sql", "/artifacts", "/databases",
  "/earnings", "/billing", "/data-types", "/settings",
];

const NAV_ORDER_AIM_DATA = [
  "/", "/datasets", "/ai-market", "/data-requests",
  "/search", "/sql", "/artifacts", "/databases",
  "/earnings", "/billing", "/data-types", "/settings",
];

// Separator index: items after this index go in the bottom section
const SEPARATOR_INDEX_DIRECT = 7;    // after Settings
const SEPARATOR_INDEX_MARKETPLACE = 2; // after Dashboard
const SEPARATOR_INDEX_AIM_DATA = 4;

function getOrderedItems(channel: "direct" | "marketplace" | "aim-data"): { top: NavItem[]; bottom: NavItem[] } {
  const order = channel === "marketplace"
    ? NAV_ORDER_MARKETPLACE
    : channel === "aim-data"
      ? NAV_ORDER_AIM_DATA
      : NAV_ORDER_DIRECT;
  const sepIdx = channel === "marketplace"
    ? SEPARATOR_INDEX_MARKETPLACE
    : channel === "aim-data"
      ? SEPARATOR_INDEX_AIM_DATA
      : SEPARATOR_INDEX_DIRECT;
  const itemMap = new Map(ALL_NAV_ITEMS.map((item) => [item.path, item]));

  const ordered = order.map((path) => itemMap.get(path)).filter(Boolean) as NavItem[];
  return {
    top: ordered.slice(0, sepIdx),
    bottom: ordered.slice(sepIdx),
  };
}

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
}

const Sidebar = ({ collapsed, onToggle }: SidebarProps) => {
  const location = useLocation();
  const { hasFeature, isStandalone } = useMode();
  const channel = useChannel();
  const brand = useBrand();

  const { top, bottom } = getOrderedItems(channel);

  const filterByFeature = (items: NavItem[]) =>
    items.filter((item) => !item.feature || hasFeature(item.feature));

  const navItems = filterByFeature(top);
  const bottomItems = filterByFeature(bottom);

  const renderNavItem = (item: NavItem) => {
    const isActive = location.pathname === item.path;
    return (
      <li key={item.path}>
        <NavLink
          to={item.path}
          className={cn(
            "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200",
            "text-sidebar-foreground hover:text-sidebar-accent-foreground hover:bg-sidebar-accent",
            isActive && "bg-sidebar-accent text-primary font-medium"
          )}
        >
          <item.icon className={cn("w-5 h-5 flex-shrink-0", isActive && "text-primary")} />
          {!collapsed && <span>{item.label}</span>}
        </NavLink>
      </li>
    );
  };

  return (
    <aside
      className={cn(
        "fixed left-0 top-0 h-screen bg-sidebar-background border-r border-sidebar-border sidebar-transition z-50 flex flex-col",
        collapsed ? "w-[60px]" : "w-[240px]"
      )}
    >
      {/* Logo */}
      <div className="h-16 flex items-center px-4 border-b border-sidebar-border">
        <a href={brand.externalUrl} target="_blank" rel="noopener noreferrer" className="flex items-center gap-3 hover:opacity-80 transition-opacity">
          <img
            src={brand.logoPath}
            alt={brand.sidebarLogoAlt}
            className="w-8 h-8 rounded-lg object-cover"
          />
          {!collapsed && (
            <span className="text-foreground font-semibold text-lg tracking-tight">{brand.name}</span>
          )}
        </a>
      </div>

      {/* Top Navigation */}
      <nav className="flex-1 py-4 px-2">
        <ul className="space-y-1">
          {navItems.map(renderNavItem)}
        </ul>
      </nav>

      {/* Bottom Navigation + Mode Indicator + Collapse */}
      <div className="px-2 pb-2 space-y-1">
        {/* Bottom nav items with separator */}
        <div className="border-t border-sidebar-border pt-2 space-y-1">
          <ul className="space-y-1">
            {bottomItems.map(renderNavItem)}
          </ul>
        </div>

        {/* Mode Indicator + Collapse Toggle */}
        <div className="pt-1 border-t border-sidebar-border space-y-1">
          {/* Mode badge */}
          <div
            className={cn(
              "flex items-center gap-2 px-3 py-1.5 rounded-lg",
              collapsed ? "justify-center" : ""
            )}
          >
            {isStandalone ? (
              <>
                <Monitor className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                {!collapsed && (
                  <span className="text-xs text-muted-foreground">Standalone Mode</span>
                )}
              </>
            ) : (
              <>
                <Globe className="w-4 h-4 text-primary flex-shrink-0" />
                {!collapsed && (
                  <span className="text-xs text-primary">Connected to ai.market</span>
                )}
              </>
            )}
          </div>
          <button
            onClick={onToggle}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-sidebar-foreground hover:text-sidebar-accent-foreground hover:bg-sidebar-accent transition-colors"
          >
            {collapsed ? (
              <ChevronRight className="w-5 h-5" />
            ) : (
              <>
                <ChevronLeft className="w-5 h-5" />
                <span>Collapse</span>
              </>
            )}
          </button>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
