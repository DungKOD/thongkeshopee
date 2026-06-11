import { TabButton } from "./AppShellButtons";

export type AppTab = "stats" | "overview" | "download" | "upload";

interface AppTabsNavProps {
  activeTab: AppTab;
  onChange: (tab: AppTab) => void;
}

export function AppTabsNav({ activeTab, onChange }: AppTabsNavProps) {
  return (
    <nav className="flex gap-1 px-6">
      <TabButton
        active={activeTab === "stats"}
        onClick={() => onChange("stats")}
        icon="analytics"
        label="Thống kê"
      />
      <TabButton
        active={activeTab === "overview"}
        onClick={() => onChange("overview")}
        icon="insights"
        label="Tổng quan"
      />
      <TabButton
        active={activeTab === "download"}
        onClick={() => onChange("download")}
        icon="download"
        label="Download video"
      />
      <TabButton
        active={activeTab === "upload"}
        onClick={() => onChange("upload")}
        icon="upload"
        label="Upload Page"
      />
    </nav>
  );
}
