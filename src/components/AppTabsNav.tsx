import { useCallback } from "react";
import { TabButton } from "./AppShellButtons";

export type AppTab = "stats" | "overview" | "download" | "upload" | "bulkcamp";

interface AppTabsNavProps {
  activeTab: AppTab;
  onChange: (tab: AppTab) => void;
}

export function AppTabsNav({ activeTab, onChange }: AppTabsNavProps) {
  // Stable onClick handlers → TabButton memo skip re-render khi parent
  // re-render do state khác (subIdQuery, account filter, etc.).
  const onStats = useCallback(() => onChange("stats"), [onChange]);
  const onOverview = useCallback(() => onChange("overview"), [onChange]);
  const onDownload = useCallback(() => onChange("download"), [onChange]);
  const onUpload = useCallback(() => onChange("upload"), [onChange]);
  const onBulkCamp = useCallback(() => onChange("bulkcamp"), [onChange]);
  return (
    <nav className="flex gap-1 px-6">
      <TabButton
        active={activeTab === "stats"}
        onClick={onStats}
        icon="analytics"
        label="Thống kê"
      />
      <TabButton
        active={activeTab === "overview"}
        onClick={onOverview}
        icon="insights"
        label="Tổng quan"
      />
      <TabButton
        active={activeTab === "download"}
        onClick={onDownload}
        icon="download"
        label="Download video"
      />
      <TabButton
        active={activeTab === "upload"}
        onClick={onUpload}
        icon="upload"
        label="Upload Page"
      />
      <TabButton
        active={activeTab === "bulkcamp"}
        onClick={onBulkCamp}
        icon="campaign"
        label="Bulk Camp"
      />
    </nav>
  );
}
