/**
 * BQ-VZ-SHARED-SEARCH: Portal Layout
 *
 * Minimal layout for the shared search portal.
 * No admin nav, no sidebar — just logo + content + footer.
 */

import { Outlet } from "react-router-dom";
import PortalAllAIChat from "./PortalAllAIChat";

const PortalLayout = () => {
  return (
    <div className="min-h-screen flex flex-col bg-background">
      {/* Header */}
      <header className="border-b border-border px-6 py-4">
        <div className="max-w-4xl mx-auto flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
            <span className="text-primary-foreground font-bold text-sm">V</span>
          </div>
          <h1 className="text-lg font-semibold text-foreground">Search Portal</h1>
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
          <span className="font-medium text-foreground">vectorAIz</span>
        </p>
      </footer>

      {/* allAI Chat (Phase 1.5) */}
      <PortalAllAIChat />
    </div>
  );
};

export default PortalLayout;
