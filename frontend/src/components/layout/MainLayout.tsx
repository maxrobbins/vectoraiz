import { useState } from "react";
import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";
import { cn } from "@/lib/utils";
import CommandPalette from "@/components/CommandPalette";
import KeyboardShortcutsModal from "@/components/KeyboardShortcutsModal";
import FileUploadModal from "@/components/FileUploadModal";
import useKeyboardShortcuts from "@/hooks/useKeyboardShortcuts";
import { useUpload } from "@/contexts/UploadContext";
import VersionBadge from "@/components/VersionBadge";
import OnboardingWizard, { isOnboardingComplete } from "@/components/onboarding/OnboardingWizard";
import { useChannel } from "@/hooks/useChannel";

const MainLayout = () => {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);
  const [helpModalOpen, setHelpModalOpen] = useState(false);
  const [showOnboarding, setShowOnboarding] = useState(() => !isOnboardingComplete());
  const channel = useChannel();
  const { openModal } = useUpload();

  useKeyboardShortcuts({
    onCommandPalette: () => setCommandPaletteOpen(true),
    onUploadModal: openModal,
    onHelpModal: () => setHelpModalOpen(true),
  });

  return (
    <div className="min-h-screen bg-background">
      <Sidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed(!sidebarCollapsed)}
      />

      <div
        className={cn(
          "sidebar-transition",
          sidebarCollapsed ? "ml-[60px]" : "ml-[240px]"
        )}
      >
        <TopBar onOpenCommandPalette={() => setCommandPaletteOpen(true)} />
        <main className="p-6">
          <Outlet />
        </main>
      </div>

      {/* Global Modals */}
      <CommandPalette
        open={commandPaletteOpen}
        onOpenChange={setCommandPaletteOpen}
        onOpenUpload={openModal}
      />
      <KeyboardShortcutsModal
        open={helpModalOpen}
        onOpenChange={setHelpModalOpen}
      />
      <FileUploadModal />
      <VersionBadge />
      {showOnboarding && (
        <OnboardingWizard channel={channel} onComplete={() => setShowOnboarding(false)} />
      )}
    </div>
  );
};

export default MainLayout;
