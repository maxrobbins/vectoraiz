import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Loader2, AlertCircle } from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";
import { toast } from "@/hooks/use-toast";
import { getApiUrl } from "@/lib/api";
import VersionBadge from "@/components/VersionBadge";

const SetupPage = () => {
  const navigate = useNavigate();
  const { setup } = useAuth();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [checking, setChecking] = useState(true);

  // Check if setup is still available (no admin exists yet)
  useEffect(() => {
    const checkSetup = async () => {
      try {
        const res = await fetch(`${getApiUrl()}/api/auth/setup`);
        if (res.ok) {
          const data = await res.json();
          if (!data.available) {
            toast({
              title: "Admin already configured",
              description: "Redirecting to login...",
            });
            navigate("/login", { replace: true });
            return;
          }
        }
      } catch {
        // Network error — show the form anyway, the POST will fail with a clear message
      } finally {
        setChecking(false);
      }
    };
    checkSetup();
  }, [navigate]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (username.length < 3) {
      setError("Username must be at least 3 characters");
      return;
    }
    if (password.length < 8) {
      setError("Password must be at least 8 characters");
      return;
    }
    if (password !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }

    setIsSubmitting(true);
    try {
      await setup(username, password);
      toast({
        title: "Welcome to vectorAIz!",
        description: "Your admin account is ready.",
      });
      navigate("/", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Setup failed");
    } finally {
      setIsSubmitting(false);
    }
  };

  if (checking) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <img src="/vectoraiz-logo-sm.png" alt="vectorAIz" className="w-10 h-10 rounded-xl animate-pulse" />
      </div>
    );
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <div className="w-full max-w-md space-y-8">
        {/* Logo + heading */}
        <div className="flex flex-col items-center gap-4">
          <img src="/vectoraiz-logo-sm.png" alt="vectorAIz" className="w-14 h-14 rounded-2xl" />
          <div className="text-center space-y-1">
            <h1 className="text-2xl font-bold text-foreground">Welcome to vectorAIz</h1>
            <p className="text-muted-foreground">Create your admin account to get started</p>
          </div>
        </div>

        <Card className="bg-card border-border">
          <CardHeader>
            <CardTitle className="text-foreground">Admin Setup</CardTitle>
            <CardDescription>This account will have full access to your instance</CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="username">Username</Label>
                <Input
                  id="username"
                  type="text"
                  placeholder="admin"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="bg-background border-border"
                  autoFocus
                  autoComplete="username"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <Input
                  id="password"
                  type="password"
                  placeholder="Min. 8 characters"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="bg-background border-border"
                  autoComplete="new-password"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="confirm-password">Confirm Password</Label>
                <Input
                  id="confirm-password"
                  type="password"
                  placeholder="Re-enter password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="bg-background border-border"
                  autoComplete="new-password"
                />
              </div>

              {error && (
                <div className="flex items-center gap-2 text-sm text-destructive">
                  <AlertCircle className="w-4 h-4 flex-shrink-0" />
                  {error}
                </div>
              )}

              <Button type="submit" className="w-full" disabled={isSubmitting}>
                {isSubmitting ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Creating account...
                  </>
                ) : (
                  "Create Admin Account"
                )}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
      <VersionBadge />
    </div>
  );
};

export default SetupPage;
