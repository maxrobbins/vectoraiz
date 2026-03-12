/**
 * BQ-VZ-SHARED-SEARCH: Portal Auth Page
 *
 * Access code entry form for the "code" tier.
 * Redirects to /portal/search after successful auth.
 */

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2, Lock, AlertCircle, LogIn } from "lucide-react";
import { usePortalAuth } from "@/hooks/usePortalAuth";
import { portalApi } from "@/api/portalApi";

const PortalAuth = () => {
  const navigate = useNavigate();
  const { config, login, error: authError } = usePortalAuth();
  const [code, setCode] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  const error = localError || authError;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!code.trim()) return;

    setIsSubmitting(true);
    setLocalError(null);
    try {
      await login(code);
      navigate("/portal/search", { replace: true });
    } catch {
      // Error is set by the hook
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSSOLogin = () => {
    portalApi.initiateSSO();
  };

  // SSO tier: show "Sign in with SSO" button
  if (config?.tier === "sso") {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <Card className="w-full max-w-md bg-card border-border">
          <CardHeader className="text-center">
            <div className="mx-auto w-12 h-12 rounded-full bg-secondary flex items-center justify-center mb-4">
              <LogIn className="w-6 h-6 text-muted-foreground" />
            </div>
            <CardTitle>Sign In Required</CardTitle>
            <CardDescription>
              Sign in with your organization account to access this portal.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {error && (
              <Alert variant="destructive" className="mb-4">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
            <Button onClick={handleSSOLogin} className="w-full">
              <LogIn className="w-4 h-4 mr-2" />
              Sign in with SSO
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Code tier: show access code form
  return (
    <div className="flex items-center justify-center min-h-[60vh]">
      <Card className="w-full max-w-md bg-card border-border">
        <CardHeader className="text-center">
          <div className="mx-auto w-12 h-12 rounded-full bg-secondary flex items-center justify-center mb-4">
            <Lock className="w-6 h-6 text-muted-foreground" />
          </div>
          <CardTitle>Access Required</CardTitle>
          <CardDescription>
            Enter the shared access code to search this portal.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            {error && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
            <Input
              type="password"
              placeholder="Access code"
              value={code}
              onChange={(e) => setCode(e.target.value)}
              disabled={isSubmitting}
              autoFocus
              className="bg-secondary border-border"
            />
            <Button type="submit" className="w-full" disabled={isSubmitting || !code.trim()}>
              {isSubmitting ? (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Lock className="w-4 h-4 mr-2" />
              )}
              Enter Portal
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
};

export default PortalAuth;
