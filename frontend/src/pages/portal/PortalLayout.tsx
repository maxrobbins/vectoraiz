/**
 * BQ-VZ-SHARED-SEARCH: Portal Layout
 *
 * Minimal layout for the shared search portal.
 * No admin nav, no sidebar — just logo + content + footer.
 */

import { Outlet } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { LogOut } from "lucide-react";
import { useBrand } from "@/contexts/BrandContext";
import { usePortalAuth } from "@/hooks/usePortalAuth";
import PortalAllAIChat from "./PortalAllAIChat";

const PortalLayout = () => {
  const { config, ssoUser, logout } = usePortalAuth();
  const brand = useBrand();

  return (
    <div className="min-h-screen flex flex-col bg-background">
      {/* Header */}
      <header className="border-b border-border px-6 py-4">
        <div className="max-w-4xl mx-auto flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
            <span className="text-primary-foreground font-bold text-sm">{brand.shortName}</span>
          </div>
          <h1 className="text-lg font-semibold text-foreground">{brand.productName} Search Portal</h1>
          {/* SSO user info */}
          {config?.tier === "sso" && ssoUser && (
            <div className="ml-auto flex items-center gap-3">
              <span className="text-sm text-muted-foreground">
                {ssoUser.name || ssoUser.email || "SSO User"}
              </span>
              <Button variant="ghost" size="sm" onClick={logout}>
                <LogOut className="w-4 h-4 mr-1" />
                Sign out
              </Button>
            </div>
          )}
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 px-6 py-8">
        <div className="max-w-4xl mx-auto">
          <Outlet />
        </div>
      </main>

      {/* Footer */}
      <footer className="border-t border-border px-6 py-4 text-center">
        <p className="text-sm text-muted-foreground">
          Powered by{" "}
          <span className="font-medium text-foreground">{brand.productName}</span>
        </p>
      </footer>

      {/* allAI Chat (Phase 1.5) */}
      <PortalAllAIChat />
    </div>
  );
};

export default PortalLayout;
