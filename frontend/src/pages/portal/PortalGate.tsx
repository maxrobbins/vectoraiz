/**
 * BQ-VZ-SHARED-SEARCH: Portal Gate
 *
 * Checks portal config and redirects:
 * - Portal disabled → shows "not available" message
 * - Code tier + not authenticated → redirects to /portal/auth
 * - Open tier or authenticated → shows children (Outlet)
 */

import { Navigate, Outlet } from "react-router-dom";
import { usePortalAuth } from "@/hooks/usePortalAuth";
import { Loader2 } from "lucide-react";

const PortalGate = () => {
  const { config, isLoading, isAuthenticated } = usePortalAuth();

  if (isLoading) {
    return (
      <div className="min-h-[60vh] flex items-center justify-center">
        <Loader2 className="w-10 h-10 text-primary animate-spin" />
      </div>
    );
  }

  if (!config || !config.enabled) {
    return (
      <div className="min-h-[60vh] flex items-center justify-center text-center">
        <div>
          <h2 className="text-xl font-semibold text-foreground mb-2">Portal Not Available</h2>
          <p className="text-muted-foreground">
            This search portal is not currently enabled.
          </p>
        </div>
      </div>
    );
  }

  // Code or SSO tier: redirect to auth if not authenticated
  if ((config.tier === "code" || config.tier === "sso") && !isAuthenticated) {
    return <Navigate to="/portal/auth" replace />;
  }

  return <Outlet />;
};

export default PortalGate;
