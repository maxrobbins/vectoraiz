import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { ModeProvider, useMode } from "./contexts/ModeContext";
import { MarketplaceProvider } from "./contexts/MarketplaceContext";
import { UploadProvider } from "./contexts/UploadContext";
import { CoPilotProvider } from "./contexts/CoPilotContext";
import ErrorBoundary from "./components/ErrorBoundary";
import MainLayout from "./components/layout/MainLayout";
import ChatPanel from "./components/copilot/ChatPanel";
import CoPilotFab from "./components/copilot/CoPilotFab";
import Dashboard from "./pages/Dashboard";
import Datasets from "./pages/Datasets";
import DatasetDetail from "./pages/DatasetDetail";
import EarningsPage from "./pages/EarningsPage";
import SearchPage from "./pages/SearchPage";
import SqlQuery from "./pages/SqlQuery";
import ArtifactsPage from "./pages/ArtifactsPage";
import DatabasePage from "./pages/DatabasePage";
import SettingsPage from "./pages/SettingsPage";
import DataTypesPage from "./pages/DataTypesPage";
import AiMarketPage from "./pages/AiMarketPage";
import SetupPage from "./pages/SetupPage";
import LoginPage from "./pages/LoginPage";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient();

/** Redirects to /login when not authenticated. Shows nothing while auth is loading. */
const RequireAuth = ({ children }: { children: React.ReactNode }) => {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="w-10 h-10 rounded-xl bg-primary flex items-center justify-center animate-pulse">
          <span className="text-primary-foreground font-bold text-lg">V</span>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
};

/** Redirects to / when a required feature flag is disabled. */
const RequireFeature = ({ feature, children }: { feature: "marketplace" | "allai" | "earnings"; children: React.ReactNode }) => {
  const { hasFeature } = useMode();
  if (!hasFeature(feature)) {
    return <Navigate to="/" replace />;
  }
  return <>{children}</>;
};

const App = () => (
  <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <BrowserRouter>
          <AuthProvider>
            <ModeProvider>
              <CoPilotProvider>
                <Toaster />
                <Sonner />
                <Routes>
                  {/* Public routes — outside MainLayout */}
                  <Route path="/setup" element={<SetupPage />} />
                  <Route path="/login" element={<LoginPage />} />

                  {/* Protected routes — inside MainLayout */}
                  <Route
                    element={
                      <RequireAuth>
                        <MarketplaceProvider>
                          <UploadProvider>
                            <ChatPanel />
                            <CoPilotFab />
                            <MainLayout />
                          </UploadProvider>
                        </MarketplaceProvider>
                      </RequireAuth>
                    }
                  >
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/datasets" element={<Datasets />} />
                    <Route path="/datasets/:id" element={<DatasetDetail />} />
                    <Route path="/earnings" element={<EarningsPage />} />
                    <Route path="/search" element={<SearchPage />} />
                    <Route path="/sql" element={<SqlQuery />} />
                    <Route path="/artifacts" element={<ArtifactsPage />} />
                    <Route path="/databases" element={<DatabasePage />} />
                    <Route path="/settings" element={<SettingsPage />} />
                    <Route path="/data-types" element={<DataTypesPage />} />
                    <Route path="/ai-market" element={<AiMarketPage />} />
                  </Route>

                  <Route path="*" element={<NotFound />} />
                </Routes>
              </CoPilotProvider>
            </ModeProvider>
          </AuthProvider>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </ErrorBoundary>
);

export default App;
