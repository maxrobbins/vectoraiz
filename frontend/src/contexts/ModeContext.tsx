import React, { createContext, useContext, useState, useEffect, useCallback } from "react";
import { getApiUrl } from "@/lib/api";
import { setMarketplaceApiUrl } from "@/lib/data-requests-api";

type Mode = "standalone" | "connected";
type Channel = "direct" | "marketplace" | "aim-data";

interface Features {
  allai: boolean;
  marketplace: boolean;
  earnings: boolean;
  local_auth: boolean;
}

interface ModeContextType {
  mode: Mode;
  channel: Channel;
  version: string;
  features: Features;
  isStandalone: boolean;
  isConnected: boolean;
  hasFeature: (name: keyof Features) => boolean;
  isLoading: boolean;
}

const DEFAULT_FEATURES: Features = { allai: false, marketplace: false, earnings: false, local_auth: true };

const ModeContext = createContext<ModeContextType | undefined>(undefined);

export const ModeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [mode, setMode] = useState<Mode>("standalone");
  const [channel, setChannel] = useState<Channel>("direct");
  const [version, setVersion] = useState("0.0.0");
  const [features, setFeatures] = useState<Features>(DEFAULT_FEATURES);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchInfo = async () => {
      try {
        const res = await fetch(`${getApiUrl()}/api/system/info`);
        if (res.ok) {
          const data = await res.json();
          setMode(data.mode ?? "standalone");
          setChannel(
            data.channel === "marketplace"
              ? "marketplace"
              : data.channel === "aim-data"
                ? "aim-data"
                : "direct"
          );
          setVersion(data.version ?? "0.0.0");
          setFeatures({ ...DEFAULT_FEATURES, ...data.features });
          setMarketplaceApiUrl(data.marketplace_api_url || null);
        }
      } catch {
        // Offline — keep defaults
      } finally {
        setIsLoading(false);
      }
    };
    fetchInfo();
  }, []);

  const hasFeature = useCallback((name: keyof Features) => !!features[name], [features]);

  return (
    <ModeContext.Provider
      value={{
        mode,
        channel,
        version,
        features,
        isStandalone: mode === "standalone",
        isConnected: mode === "connected",
        hasFeature,
        isLoading,
      }}
    >
      {children}
    </ModeContext.Provider>
  );
};

export const useMode = (): ModeContextType => {
  const context = useContext(ModeContext);
  if (!context) {
    throw new Error("useMode must be used within a ModeProvider");
  }
  return context;
};
