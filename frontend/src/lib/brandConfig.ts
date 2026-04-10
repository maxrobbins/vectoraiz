export type BrandConfig = {
  name: string;
  tagline: string;
  logoPath: string;
  logoSmPath: string;
  metaTitle: string;
  metaDescription: string;
  metaAuthor: string;
  ogTitle: string;
  twitterSite: string;
  sidebarLogoAlt: string;
  welcomeTitle: string;
  settingsTitle: string;
  externalUrl: string;
};

export const VECTORAIZ_BRAND: BrandConfig = {
  name: "vectorAIz",
  tagline: "Your local data processing and semantic search tool",
  logoPath: "/vectoraiz-logo.jpg",
  logoSmPath: "/vectoraiz-logo-sm.png",
  metaTitle: "vectorAIz — Data Processing & Analysis",
  metaDescription: "vectorAIz - Your local data processing and semantic search tool",
  metaAuthor: "vectorAIz",
  ogTitle: "vectorAIz",
  twitterSite: "@vectorAIz",
  sidebarLogoAlt: "vectorAIz",
  welcomeTitle: "Welcome to vectorAIz",
  settingsTitle: "vectorAIz",
  externalUrl: "https://vectoraiz.com",
};

export const AIM_CHANNEL_BRAND: BrandConfig = {
  name: "AIM Channel",
  tagline: "Data Channel for ai.market",
  logoPath: "/aim-channel-logo.jpg",
  logoSmPath: "/aim-channel-logo-sm.png",
  metaTitle: "AIM Channel — Data Processing & Analysis",
  metaDescription: "AIM Channel - Connect your private data to ai.market",
  metaAuthor: "AIM Channel",
  ogTitle: "AIM Channel",
  twitterSite: "@aidotmarket",
  sidebarLogoAlt: "AIM Channel",
  welcomeTitle: "Welcome to AIM Channel",
  settingsTitle: "AIM Channel",
  externalUrl: "https://ai.market",
};

function getRuntimeBrandName(): string {
  const envBrand = import.meta.env.VITE_BRAND?.toLowerCase();
  if (envBrand === "aim-channel" || envBrand === "aim_channel" || envBrand === "aim") {
    return "aim-channel";
  }
  if (envBrand === "vectoraiz") {
    return "vectoraiz";
  }

  if (typeof window !== "undefined" && window.location.hostname.includes("ai.market")) {
    return "aim-channel";
  }

  return "vectoraiz";
}

export function getActiveBrand(): BrandConfig {
  return getRuntimeBrandName() === "aim-channel" ? AIM_CHANNEL_BRAND : VECTORAIZ_BRAND;
}
