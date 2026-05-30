/**
 * BQ-VZ-SHARED-SEARCH: Portal Session Management Hook
 *
 * Manages portal auth state (separate from admin auth context).
 * Handles open tier (auto-authenticated) and code tier (requires access code).
 */

import { useState, useEffect, useCallback } from "react";
import {
  portalApi,
  getPortalToken,
  setPortalToken,
  clearPortalToken,
  type PortalPublicConfig,
  type PortalSSOUserInfo,
} from "@/api/portalApi";

export interface PortalAuthState {
  config: PortalPublicConfig | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  error: string | null;
  ssoUser: PortalSSOUserInfo | null;
  login: (code: string) => Promise<void>;
  logout: () => void;
}

export function usePortalAuth(): PortalAuthState {
  const [config, setConfig] = useState<PortalPublicConfig | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [ssoUser, setSsoUser] = useState<PortalSSOUserInfo | null>(null);

  // Load portal config on mount
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        // Check for SSO callback token in URL fragment
        const hash = window.location.hash;
        if (hash.includes("sso_token=")) {
          const token = hash.split("sso_token=")[1]?.split("&")[0];
          if (token) {
            setPortalToken(token);
            // Clean up URL fragment
            window.history.replaceState(null, "", window.location.pathname + window.location.search);
          }
        }

        const cfg = await portalApi.getConfig();
        if (cancelled) return;
        setConfig(cfg);

        // Open tier = auto-authenticated
        if (cfg.tier === "open" && cfg.enabled) {
          setIsAuthenticated(true);
        }
        // Code tier: check if we have a valid token
        else if (cfg.tier === "code" && getPortalToken()) {
          try {
            await portalApi.getDatasets();
            if (!cancelled) setIsAuthenticated(true);
          } catch {
            clearPortalToken();
          }
        }
        // SSO tier: check if we have a valid token (from callback or session)
        else if (cfg.tier === "sso" && getPortalToken()) {
          try {
            const userInfo = await portalApi.getSSOUserInfo();
            if (!cancelled) {
              setIsAuthenticated(true);
              setSsoUser(userInfo);
            }
          } catch {
            clearPortalToken();
          }
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load portal config");
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const login = useCallback(async (code: string) => {
    setError(null);
    try {
      await portalApi.authWithCode(code);
      setIsAuthenticated(true);
    } catch (e: any) {
      const msg = e.status === 429
        ? "Too many attempts. Please wait and try again."
        : e.status === 401
          ? "Invalid access code."
          : e.message || "Authentication failed.";
      setError(msg);
      throw e;
    }
  }, []);

  const logout = useCallback(async () => {
    // For SSO, call the logout endpoint
    if (config?.tier === "sso" && getPortalToken()) {
      try {
        const res = await portalApi.ssoLogout();
        // Optionally redirect to IdP logout
        if (res.end_session_url) {
          window.location.href = res.end_session_url;
          return;
        }
      } catch {
        // Clear locally even if server logout fails
      }
    }
    clearPortalToken();
    setIsAuthenticated(false);
    setSsoUser(null);
  }, [config]);

  return { config, isLoading, isAuthenticated, error, ssoUser, login, logout };
}
