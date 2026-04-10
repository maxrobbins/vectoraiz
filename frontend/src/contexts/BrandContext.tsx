import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { getActiveBrand, type BrandConfig } from "@/lib/brandConfig";

const BrandContext = createContext<BrandConfig | null>(null);

export function BrandProvider({ children }: { children: ReactNode }) {
  const [brand, setBrand] = useState<BrandConfig>(() => getActiveBrand());

  useEffect(() => {
    setBrand(getActiveBrand());
  }, []);

  return <BrandContext.Provider value={brand}>{children}</BrandContext.Provider>;
}

export function useBrand(): BrandConfig {
  const context = useContext(BrandContext);

  if (!context) {
    throw new Error("useBrand must be used within a BrandProvider");
  }

  return context;
}
