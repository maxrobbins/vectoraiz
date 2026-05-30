import { useEffect } from "react";
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { BrandProvider, useBrand } from "./contexts/BrandContext";
import { ModeProvider, useMode } from "./contexts/ModeContext";
import { MarketplaceProvider } from "./contexts/MarketplaceContext";
import { UploadProvider } from "./contexts/UploadContext";
import { CoPilotProvider } from "./contexts/CoPilotContext";
import ErrorBoundary from "./components/ErrorBoundary";
import MainLayout from "./components/layout/MainLayout";
import ChatPanel from "./components/copilot/ChatPanel";
import CoPilotFab from "./components/copilot/CoPilotFab";
import { useChannel } from "./hooks/useChannel";
import Dashboard from "./pages/Dashboard";
import Datasets from "./pages/Datasets";
import DatasetDetail from "./pages/DatasetDetail";
import EarningsPage from "./pages/EarningsPage";
import SearchPage from "./pages/SearchPage";
import SqlQuery from "./pages/SqlQuery";
import ArtifactsPage from "./pages/ArtifactsPage";
import DatabasePage from "./pages/DatabasePage";
import SettingsPage from "./pages/SettingsPage";
import BillingPage from "./pages/BillingPage";
import DataTypesPage from "./pages/DataTypesPage";
import AiMarketPage from "./pages/AiMarketPage";
import DataRequestsPage from "./pages/DataRequestsPage";
import CreateDataRequestPage from "./pages/CreateDataRequestPage";
import DataRequestDetailPage from "./pages/DataRequestDetailPage";
import DashboardRequestsPage from "./pages/DashboardRequestsPage";
import SetupPage from "./pages/SetupPage";
import LoginPage from "./pages/LoginPage";
import RawFileDetail from "./pages/RawFileDetail";
import NotFound from "./pages/NotFound";
import PortalLayout from "./pages/portal/PortalLayout";
import PortalGate from "./pages/portal/PortalGate";
import PortalAuth from "./pages/portal/PortalAuth";
import PortalSearch from "./pages/portal/PortalSearch";
import PortalDatasets from "./pages/portal/PortalDatasets";

const queryClient = new QueryClient();

const BrandEffects = () => {
  const brand = useBrand();

  useEffect(() => {
    document.title = brand.metaTitle;

    const setMetaContent = (selector: string, content: string) => {
      const tag = document.head.querySelector<HTMLMetaElement>(selector);
      if (tag) {
        tag.content = content;
      }
    };

    setMetaContent('meta[name="description"]', brand.metaDescription);
    setMetaContent('meta[name="author"]', brand.metaAuthor);
    setMetaContent('meta[property="og:title"]', brand.ogTitle);
    setMetaContent('meta[property="og:description"]', brand.metaDescription);
    setMetaContent('meta[name="twitter:site"]', brand.twitterSite);
  }, [brand]);

  return null;
};

/** Redirects to /login when not authenticated. Shows nothing while auth is loading. */
const RequireAuth = ({ children }: { children: React.ReactNode }) => {
  const { isAuthenticated, isLoading } = useAuth();
  const brand = useBrand();

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="w-10 h-10 rounded-xl bg-primary flex items-center justify-center animate-pulse">
          <span className="text-primary-foreground font-bold text-lg">{brand.shortName}</span>
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

/** Channel-aware landing page: marketplace → /ai-market, aim-data/direct → /datasets */
const ChannelLanding = () => {
  const channel = useChannel();
  const target = channel === "marketplace" ? "/ai-market" : "/datasets";
  return <Navigate to={target} replace />;
};

const App = () => (
  <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <BrandProvider>
          <BrandEffects />
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

                  {/* Portal routes — own layout, own auth (BQ-VZ-SHARED-SEARCH) */}
                  <Route path="/portal" element={<PortalLayout />}>
                    <Route path="auth" element={<PortalAuth />} />
                    <Route element={<PortalGate />}>
                      <Route index element={<PortalSearch />} />
                      <Route path="datasets" element={<PortalDatasets />} />
                    </Route>
                  </Route>

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
                    <Route path="/" element={<ChannelLanding />} />
                    <Route path="/datasets" element={<Datasets />} />
                    <Route path="/datasets/:id" element={<DatasetDetail />} />
                    <Route path="/raw-files/:id" element={<RawFileDetail />} />
                    <Route path="/earnings" element={<EarningsPage />} />
                    <Route path="/search" element={<SearchPage />} />
                    <Route path="/sql" element={<SqlQuery />} />
                    <Route path="/artifacts" element={<ArtifactsPage />} />
                    <Route path="/databases" element={<DatabasePage />} />
                    <Route path="/settings" element={<SettingsPage />} />
                    <Route path="/billing" element={<BillingPage />} />
                    <Route path="/data-types" element={<DataTypesPage />} />
                    <Route path="/ai-market" element={<AiMarketPage />} />
                    <Route path="/data-requests" element={<DataRequestsPage />} />
                    <Route path="/data-requests/new" element={<CreateDataRequestPage />} />
                    <Route path="/data-requests/:slug" element={<DataRequestDetailPage />} />
                    <Route path="/my-requests" element={<DashboardRequestsPage />} />
                  </Route>

                  <Route path="*" element={<NotFound />} />
                  </Routes>
                </CoPilotProvider>
              </ModeProvider>
            </AuthProvider>
          </BrowserRouter>
        </BrandProvider>
      </TooltipProvider>
    </QueryClientProvider>
  </ErrorBoundary>
);

export default App;
