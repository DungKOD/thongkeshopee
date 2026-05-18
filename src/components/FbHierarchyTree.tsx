import { Fragment } from "react";
import type { FbBreakdown, FbAdSetGroup, FbAdLeaf } from "../types";
import { fmtInt, fmtVnd } from "../formulas";

interface FbHierarchyTreeProps {
  breakdown: FbBreakdown;
  showAccount: boolean;
}

const NA = "—";
const naCls = "text-white/30";
const cell = "px-3 py-1.5 text-center tabular-nums text-sm";

function VndCell({ v, cls = "" }: { v: number | null | undefined; cls?: string }) {
  if (!v) return <td className={`${cell} ${naCls}`}>{NA}</td>;
  return <td className={`${cell} ${cls}`}>{fmtVnd(v)}</td>;
}

function IntCell({ v }: { v: number | null | undefined }) {
  if (v === null || v === undefined) return <td className={`${cell} ${naCls}`}>{NA}</td>;
  return <td className={cell}>{fmtInt(v)}</td>;
}

function NaCell() {
  return <td className={`${cell} ${naCls}`}>{NA}</td>;
}

/**
 * Tính cấp nào cần render dựa trên cấu trúc thực tế.
 *
 * showCampaign=false → chỉ 1 campaign → dòng camp trùng data với main row → bỏ
 * showAdSet=false    → mọi campaign chỉ có 1 adset → adset redundant → bỏ
 * showAd=false       → mọi adset chỉ có 1 ad      → ad redundant → bỏ
 *
 *  1 camp, 1-1 → không sub-row nào (main row đã đủ)
 *  1 camp, 2-1 → 2 nhóm trực tiếp (bỏ camp)
 *  1 camp, 2-2 → 2 nhóm + ads (bỏ camp)
 *  1 camp, 1-2 → 2 ads trực tiếp (bỏ camp + adset)
 *  2+ camps    → hiện camp, rồi nhóm, rồi ads theo logic cũ
 */
function computeLevels(breakdown: FbBreakdown) {
  const allAdSets = breakdown.campaigns.flatMap((c) => c.adSets);
  const showCampaign = breakdown.campaigns.length > 1;
  const showAdSet = !breakdown.campaigns.every((c) => c.adSets.length === 1);
  const showAd = !allAdSets.every((a) => a.ads.length === 1);
  return { showCampaign, showAdSet, showAd };
}

/**
 * Các ô dữ liệu FB + NA cho Shopee, theo đúng thứ tự cột của bảng chính:
 *
 * Click ADS | Click Shopee | CPC | Spend | Số đơn | CR | GMV | HH | LN | ROI | Actions
 * (FB value)   (NA)         (FB)   (FB)    (NA)    (NA) (NA) (NA) (NA) (NA)
 */
function FbDataCols({
  clicks,
  cpc,
  spend,
  cpcCls,
  spendCls,
}: {
  clicks: number | null | undefined;
  cpc: number | null | undefined;
  spend: number;
  cpcCls: string;
  spendCls: string;
}) {
  return (
    <>
      <IntCell v={clicks} />                    {/* Click ADS      */}
      <NaCell />                                 {/* Click Shopee   */}
      <VndCell v={cpc} cls={cpcCls} />          {/* CPC            */}
      <VndCell v={spend} cls={spendCls} />      {/* Spend          */}
      <NaCell />                                 {/* Số đơn        */}
      <NaCell />                                 {/* CR             */}
      <NaCell />                                 {/* GMV            */}
      <NaCell />                                 {/* Hoa hồng      */}
      <NaCell />                                 {/* Lợi nhuận     */}
      <NaCell />                                 {/* ROI            */}
    </>
  );
}

function AdRow({
  ad,
  showAccount,
  indentCls,
}: {
  ad: FbAdLeaf;
  showAccount: boolean;
  indentCls: string;
}) {
  return (
    <tr className="border-b border-surface-8/20">
      <td />
      <td className="px-4 py-1.5 text-left">
        <div className={`flex items-center gap-1.5 truncate ${indentCls}`} title={ad.adName}>
          <span className="material-symbols-rounded text-[13px] text-emerald-300">ads_click</span>
          <span className="truncate text-sm text-white/70">
            {ad.adName}
            {ad.occurrenceIdx > 0 && (
              <span className="ml-1 text-white/40">(#{ad.occurrenceIdx + 1})</span>
            )}
          </span>
          <span className="shrink-0 rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-emerald-300">
            Ads
          </span>
        </div>
      </td>
      {showAccount && <td />}
      <FbDataCols
        clicks={ad.clicks}
        cpc={ad.cpc}
        spend={ad.spend}
        cpcCls="text-gray-400"
        spendCls="text-blue-200"
      />
      <td className="col-actions" />
    </tr>
  );
}

function AdSetRow({
  adset,
  showAccount,
  showAd,
  indentCls,
}: {
  adset: FbAdSetGroup;
  showAccount: boolean;
  showAd: boolean;
  indentCls: string;
}) {
  return (
    <tr className="border-b border-surface-8/40 bg-surface-1/25">
      <td />
      <td className="px-4 py-1.5 text-left">
        <div className={`flex items-center gap-1.5 truncate ${indentCls}`} title={adset.adSetName}>
          <span className="material-symbols-rounded text-[13px] text-amber-300">folder</span>
          <span className="truncate text-sm text-white/85">{adset.adSetName}</span>
          <span className="shrink-0 rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-amber-300">
            Nhóm
          </span>
          {!showAd && adset.ads[0] && (
            <span className="ml-1 shrink-0 truncate text-[11px] text-white/30" title={adset.ads[0].adName}>
              · {adset.ads[0].adName}
            </span>
          )}
        </div>
      </td>
      {showAccount && <td />}
      <FbDataCols
        clicks={adset.clicks}
        cpc={adset.cpc}
        spend={adset.spend}
        cpcCls="text-gray-400"
        spendCls="text-blue-300"
      />
      <td className="col-actions" />
    </tr>
  );
}

export function FbHierarchyTree({ breakdown, showAccount }: FbHierarchyTreeProps) {
  if (!breakdown.campaigns.length) return null;

  const { showCampaign, showAdSet, showAd } = computeLevels(breakdown);

  // Indent tự điều chỉnh: khi bỏ dòng camp (1 campaign), adset/ad nhích lên 1 cấp.
  const adsetIndent = showCampaign ? "pl-10" : "pl-4";
  const adUnderAdsetIndent = showCampaign ? "pl-16" : "pl-10";
  const adFlatIndent = showCampaign ? "pl-10" : "pl-4";

  // Không có gì cần hiện (1-1-1 với 1 camp): main row đã đủ data.
  if (!showCampaign && !showAdSet && !showAd) return null;

  return (
    <>
      {breakdown.campaigns.map((camp) => {
        // Khi không hiện adset nhưng cần hiện ad: flatten ads của camp
        const flatAds: FbAdLeaf[] = !showAdSet && showAd
          ? camp.adSets.flatMap((a) => a.ads)
          : [];

        return (
          <Fragment key={camp.campaignName}>
            {/* ── Campaign (chỉ hiện khi có ≥2 campaign) ── */}
            {showCampaign && (
              <tr className="border-b border-surface-8/60 bg-shopee-900/20">
                <td />
                <td className="px-4 py-1.5 text-left">
                  <div className="flex items-center gap-1.5 truncate pl-4" title={camp.campaignName}>
                    <span className="material-symbols-rounded text-[13px] text-shopee-400">campaign</span>
                    <span className="truncate text-sm font-semibold text-white">
                      {camp.campaignName}
                    </span>
                    <span className="shrink-0 rounded bg-shopee-900/40 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-shopee-300">
                      Camp
                    </span>
                  </div>
                </td>
                {showAccount && <td />}
                <FbDataCols
                  clicks={camp.clicks}
                  cpc={camp.cpc}
                  spend={camp.spend}
                  cpcCls="text-gray-400"
                  spendCls="font-semibold text-blue-400"
                />
                <td className="col-actions" />
              </tr>
            )}

            {/* ── Adset rows ── */}
            {showAdSet &&
              camp.adSets.map((adset) => (
                <Fragment key={adset.adSetName}>
                  <AdSetRow
                    adset={adset}
                    showAccount={showAccount}
                    showAd={showAd}
                    indentCls={adsetIndent}
                  />
                  {showAd &&
                    adset.ads.map((ad) => (
                      <AdRow
                        key={`${ad.adName}\x1f${ad.occurrenceIdx}`}
                        ad={ad}
                        showAccount={showAccount}
                        indentCls={adUnderAdsetIndent}
                      />
                    ))}
                </Fragment>
              ))}

            {/* ── Ad rows trực tiếp (adset duy nhất bị bỏ, vd 1-1-N) ── */}
            {flatAds.map((ad) => (
              <AdRow
                key={`${ad.adName}\x1f${ad.occurrenceIdx}`}
                ad={ad}
                showAccount={showAccount}
                indentCls={adFlatIndent}
              />
            ))}
          </Fragment>
        );
      })}
    </>
  );
}
