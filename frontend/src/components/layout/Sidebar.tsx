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
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useMode } from "@/contexts/ModeContext";

interface NavItem {
  path: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  feature?: "marketplace";
}

const topNavItems: NavItem[] = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/datasets", label: "Datasets", icon: Database },
  { path: "/search", label: "Search", icon: Search },
  { path: "/sql", label: "SQL Query", icon: Code },
  { path: "/artifacts", label: "Artifacts", icon: FileOutput },
  { path: "/databases", label: "Databases", icon: Database },
  { path: "/settings", label: "Settings", icon: Settings },
];

const bottomNavItems: NavItem[] = [
  { path: "/ai-market", label: "ai.market", icon: Store },
  { path: "/data-requests", label: "I Need Data", icon: HandHelping },
  { path: "/earnings", label: "Earnings", icon: DollarSign, feature: "marketplace" },
  { path: "/data-types", label: "Data Types", icon: FileStack },
];

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
}

const Sidebar = ({ collapsed, onToggle }: SidebarProps) => {
  const location = useLocation();
  const { hasFeature, isStandalone } = useMode();

  const navItems = topNavItems.filter(
    (item) => !item.feature || hasFeature(item.feature)
  );

  const bottomItems = bottomNavItems.filter(
    (item) => !item.feature || hasFeature(item.feature)
  );

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
        <a href="https://vectoraiz.com" target="_blank" rel="noopener noreferrer" className="flex items-center gap-3 hover:opacity-80 transition-opacity">
          <img
            src="/vectoraiz-logo.jpg"
            alt="vectorAIz"
            className="w-8 h-8 rounded-lg object-cover"
          />
          {!collapsed && (
            <span className="text-foreground font-semibold text-lg tracking-tight">vectorAIz</span>
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
