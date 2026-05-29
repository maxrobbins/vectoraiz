export type BrandConfig = {
  name: string;
  productName: string;
  shortName: string;
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
  installDirectoryName: string;
  dockerComposeServiceName: string;
  documentationUrl: string;
  githubUrl: string;
  issueTrackerUrl: string;
  importDir: string;
  importDirEnvVar: string;
  docsConnectedModeUrl: string;
  devApiUrl: string;
  prodApiUrl: string;
};

export const VECTORAIZ_BRAND: BrandConfig = {
  name: "vectorAIz",
  productName: "vectorAIz",
  shortName: "V",
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
  installDirectoryName: "vectoraiz",
  dockerComposeServiceName: "vectoraiz",
  documentationUrl: "https://github.com/aidotmarket/vectoraiz",
  githubUrl: "https://github.com/aidotmarket/vectoraiz",
  issueTrackerUrl: "https://github.com/aidotmarket/vectoraiz/issues",
  importDir: "~/vectoraiz-imports/",
  importDirEnvVar: "VECTORAIZ_IMPORT_DIR",
  docsConnectedModeUrl: "https://ai.market/docs/vectoraiz/connected-mode",
  devApiUrl: "https://vectoraiz-backend-production.up.railway.app",
  prodApiUrl: "",
};

export const AIM_DATA_BRAND: BrandConfig = {
  name: "AIM Data",
  productName: "AIM Data",
  shortName: "AD",
  tagline: "Connect your private data to ai.market",
  logoPath: "/aim-data-logo.svg",
  logoSmPath: "/aim-data-logo.svg",
  metaTitle: "AIM Data — ai.market",
  metaDescription: "AIM Data — Connect your private data to ai.market",
  metaAuthor: "AIM Data",
  ogTitle: "AIM Data",
  twitterSite: "@aidotmarket",
  sidebarLogoAlt: "AIM Data",
  welcomeTitle: "Welcome to AIM Data",
  settingsTitle: "AIM Data",
  externalUrl: "https://ai.market",
  installDirectoryName: "aim-data",
  dockerComposeServiceName: "vectoraiz",
  documentationUrl: "https://ai.market/docs",
  githubUrl: "https://github.com/aidotmarket/vectoraiz",
  issueTrackerUrl: "https://github.com/aidotmarket/vectoraiz/issues",
  importDir: "~/aim-data-imports/",
  importDirEnvVar: "AIM_DATA_IMPORT_DIR",
  docsConnectedModeUrl: "https://ai.market/docs/aim-data/connected-mode",
  devApiUrl: "",
  prodApiUrl: "",
};

export const AIM_CHANNEL_BRAND: BrandConfig = AIM_DATA_BRAND;

function getRuntimeBrandName(): string {
  const envBrand = import.meta.env.VITE_BRAND?.toLowerCase();
  if (
    envBrand === "aim-data" ||
    envBrand === "aim_data" ||
    envBrand === "aim-channel" ||
    envBrand === "aim_channel" ||
    envBrand === "aim"
  ) {
    return "aim-data";
  }
  if (envBrand === "vectoraiz") {
    return "vectoraiz";
  }

  if (typeof window !== "undefined" && window.location.hostname.includes("ai.market")) {
    return "aim-data";
  }

  return "aim-data";
}

export function getActiveBrand(): BrandConfig {
  return getRuntimeBrandName() === "aim-data" ? AIM_DATA_BRAND : VECTORAIZ_BRAND;
}
