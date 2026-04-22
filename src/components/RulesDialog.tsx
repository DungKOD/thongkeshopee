import { useEffect } from "react";
import { createPortal } from "react-dom";

interface RulesDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

/** Dialog chi tiết các quy tắc sử dụng app — import CSV, naming sub_id, ROI, v.v. */
export function RulesDialog({ isOpen, onClose }: RulesDialogProps) {
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="rules-dialog-title"
      >
        <header className="flex shrink-0 items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-shopee-400">
            rule
          </span>
          <h2
            id="rules-dialog-title"
            className="text-lg font-semibold text-white/90"
          >
            Quy tắc sử dụng
          </h2>
        </header>

        <div className="min-h-0 flex-1 space-y-6 overflow-y-auto px-6 py-5">
          {/* ========== Quy tắc quan trọng nhất ========== */}
          <section>
            <div
              role="alert"
              className="flex items-start gap-3 rounded-lg border-2 border-amber-400 bg-amber-950/50 px-4 py-3 shadow-lg shadow-amber-900/30"
            >
              <span className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-amber-400 text-lg font-black text-black">
                !
              </span>
              <div className="flex-1 space-y-1.5">
                <p className="text-sm font-bold uppercase tracking-wide text-amber-300">
                  Quy tắc quan trọng nhất
                </p>
                <p className="text-base font-semibold leading-snug text-white">
                  Đặt tên <span className="text-amber-300">Sub_id</span> giống{" "}
                  <span className="underline decoration-amber-400">y hệt</span>{" "}
                  tên <span className="text-amber-300">Campaign FB</span>.
                </p>
                <p className="text-xs text-white/70">
                  Campaign FB{" "}
                  <code className="rounded bg-surface-1 px-1.5 py-0.5 font-mono text-amber-200">
                    sanpham0101
                  </code>{" "}
                  → Sub_id trong link Shopee cũng phải là{" "}
                  <code className="rounded bg-surface-1 px-1.5 py-0.5 font-mono text-amber-200">
                    sanpham0101
                  </code>
                  . App mới gộp được data FB + Shopee về cùng 1 dòng.
                </p>
              </div>
            </div>
          </section>

          {/* ========== Import CSV ========== */}
          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                upload_file
              </span>
              Import CSV
            </h3>
            <div className="space-y-2 rounded-xl border border-surface-8 bg-surface-6 px-4 py-3 text-sm text-white/85">
              <RuleItem n="①">
                <strong>Mỗi file = 1 ngày</strong>. File chứa nhiều ngày (VD
                có row của 17/04 và 18/04) → app reject ngay, không ghi gì.
              </RuleItem>
              <RuleItem n="②">
                <strong>Chọn nhiều file cùng lúc</strong> → tất cả phải cùng
                ngày. Khác ngày → reject kèm danh sách file nào là ngày nào.
              </RuleItem>
              <RuleItem n="③">
                <strong>4 loại file</strong> tự nhận diện qua header:
                <ul className="mt-1 space-y-0.5 pl-4 text-xs text-white/65">
                  <li>• Click Shopee (WebsiteClickReport)</li>
                  <li>• Hoa hồng Shopee (AffiliateCommissionReport)</li>
                  <li>• FB Ad Group (Nhóm quảng cáo)</li>
                  <li>• FB Campaign (Chiến dịch)</li>
                </ul>
              </RuleItem>
              <RuleItem n="④">
                <strong>Import lại</strong> → dialog preview hiện bảng{" "}
                <span className="text-green-400">thêm mới</span> /{" "}
                <span className="text-amber-300">replace</span>. User xác nhận
                mới ghi DB.
              </RuleItem>
              <RuleItem n="⑤">
                <strong>File trùng 100%</strong> (cùng SHA-256) → reject để
                tránh import đè vô nghĩa.
              </RuleItem>
              <RuleItem n="⑥">
                <strong>Raw CSV gốc</strong> được copy vào thư mục app_data
                để rollback khi cần.
              </RuleItem>
            </div>
          </section>

          {/* ========== Đặt tên Sub_id & Campaign ========== */}
          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                tips_and_updates
              </span>
              Đặt tên Sub_id & Campaign
            </h3>
            <div className="space-y-3 rounded-xl border border-shopee-500/30 bg-shopee-900/20 px-4 py-3 text-sm text-white/85">
              <div>
                <p className="mb-1 font-semibold text-shopee-300">
                  1. Cấu trúc Sub_id (Shopee Affiliate)
                </p>
                <p className="text-xs leading-relaxed text-white/70">
                  Shopee cho 5 slot{" "}
                  <code className="text-shopee-200">Sub_id1..5</code>. Convention
                  gợi ý:
                </p>
                <ul className="mt-1.5 space-y-0.5 pl-4 text-xs text-white/65">
                  <li>
                    • <code className="text-shopee-200">Sub_id1</code> =
                    shop/account (VD <code>shop1</code>)
                  </li>
                  <li>
                    • <code className="text-shopee-200">Sub_id2</code> = slug
                    sản phẩm (VD <code>sanpham1</code>)
                  </li>
                  <li>
                    • <code className="text-shopee-200">Sub_id3</code> = mã
                    campaign/ngày (VD <code>0101</code>)
                  </li>
                  <li>
                    • <code className="text-shopee-200">Sub_id4-5</code> = biến
                    thể A/B, nguồn traffic... (optional)
                  </li>
                </ul>
              </div>

              <div>
                <p className="mb-1 font-semibold text-shopee-300">
                  2. Tên FB Campaign = Sub_id (nối bằng "-" nếu dùng nhiều slot)
                </p>
                <div className="rounded bg-surface-1 px-2 py-1 font-mono text-xs text-shopee-200">
                  sanpham0101
                </div>
                <p className="mt-1 text-xs text-white/55">
                  Đơn giản nhất: dùng 1 slot (Sub_id1 = tên camp) như trên.
                  Nếu chia thành nhiều slot (shop-sản phẩm-ngày), nối bằng{" "}
                  <code className="text-shopee-200">-</code> — app sẽ split theo
                  <code className="text-shopee-200">-</code> về Sub_id1..5 để
                  merge với Shopee.
                </p>
              </div>

              <div>
                <p className="mb-1 font-semibold text-shopee-300">
                  3. Slug KHÔNG chứa dấu "-"
                </p>
                <p className="text-xs leading-relaxed text-white/70">
                  Vì app split theo <code className="text-shopee-200">-</code>.
                </p>
                <p className="mt-1 text-xs text-white/55">
                  ✅ <code className="text-shopee-200">sanpham</code>,{" "}
                  <code className="text-shopee-200">san_pham</code>,{" "}
                  <code className="text-shopee-200">sanPham</code>
                  <br />❌ <code className="text-red-300">san-pham</code>
                </p>
              </div>

              <div>
                <p className="mb-1 font-semibold text-shopee-300">
                  4. Map theo commission là chính
                </p>
                <p className="text-xs leading-relaxed text-white/70">
                  Sub_id từ <strong>hoa hồng sản phẩm</strong> làm anchor. FB
                  camp có sub_id tương thích (prefix) sẽ gộp vào anchor. Không
                  match → đứng riêng, hiển thị nguyên tên camp.
                </p>
              </div>

              <div>
                <p className="mb-1 font-semibold text-shopee-300">
                  5. Nhất quán giữa các ngày
                </p>
                <p className="text-xs leading-relaxed text-white/70">
                  Cùng 1 SP chạy nhiều ngày → giữ sub_id1-sub_id2 cố định, chỉ
                  đổi sub_id3 (mã ngày) để thống kê lifetime chính xác.
                </p>
              </div>
            </div>
          </section>

          {/* ========== Ý nghĩa các chỉ số ========== */}
          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                calculate
              </span>
              Ý nghĩa chỉ số
            </h3>
            <div className="space-y-2 rounded-xl border border-surface-8 bg-surface-6 px-4 py-3 text-sm text-white/85">
              <MetricRow name="CPC">
                <strong>Cost Per Click</strong> — đơn giá mỗi lần có người bấm
                vào quảng cáo. <code>= Tổng tiền chạy / Click ADS</code>.
                Ưu tiên lấy số FB đã tính (weighted average theo click của
                từng ad group). Nếu FB không có → tự tính từ spend ÷ clicks.
                CPC thấp = ads rẻ, tốt cho ROI.
              </MetricRow>
              <MetricRow name="CR (Tỷ lệ chuyển đổi)">
                <code>= Số đơn / Click Shopee × 100%</code>. Dùng click Shopee
                (chứ không phải click ADS) vì user phải vào Shopee mới có khả
                năng mua.
              </MetricRow>
              <MetricRow name="Giá trị đơn hàng">
                GMV trung bình <code>= Tổng GMV / Số đơn</code>. Lấy từ raw
                <code>order_value</code> của Shopee.
              </MetricRow>
              <MetricRow name="Hoa hồng">
                Tổng net commission Shopee đã trừ phí MCN (cột "Hoa hồng ròng"
                trong CSV).
              </MetricRow>
              <MetricRow name="Lợi nhuận">
                <code>
                  = Hoa hồng × (1 − thuế) − Hoa hồng pending × dự phòng − Tiền
                  ads
                </code>
                . Thuế/phí sàn áp cho mọi đơn. <strong>Dự phòng</strong> CHỈ
                trừ từ hoa hồng của đơn <b>"Đang chờ xử lý"</b> và{" "}
                <b>"Chưa thanh toán"</b> (rủi ro bị hủy). Đơn đã hoàn thành →
                không dự phòng. Phí cấu hình trong "Phí khấu trừ lợi nhuận".
              </MetricRow>
              <MetricRow name="ROI">
                <code>= Lợi nhuận / Tiền ads × 100%</code>. Không có spend →
                "—" (lãi tự nhiên không qua ads).
              </MetricRow>
            </div>
          </section>

          {/* ========== Xóa data ========== */}
          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">delete</span>
              Xóa data (staged)
            </h3>
            <div className="space-y-2 rounded-xl border border-surface-8 bg-surface-6 px-4 py-3 text-sm text-white/85">
              <RuleItem n="①">
                Bấm xóa row/ngày → hiển thị <span className="line-through text-white/50">gạch ngang</span>,{" "}
                <strong>CHƯA</strong> ghi DB.
              </RuleItem>
              <RuleItem n="②">
                Bấm icon undo → bỏ đánh dấu.
              </RuleItem>
              <RuleItem n="③">
                Floating bar dưới màn hình đếm số thay đổi → click{" "}
                <strong>"Lưu thay đổi"</strong> để commit batch.
              </RuleItem>
              <RuleItem n="④">
                Reload app khi có pending delete → pending mất, data DB
                nguyên vẹn.
              </RuleItem>
            </div>
          </section>
        </div>

        <footer className="flex shrink-0 justify-end border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
          >
            Đã hiểu
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}

function RuleItem({
  n,
  children,
}: {
  n: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-start gap-2">
      <span className="mt-0.5 shrink-0 text-shopee-400">{n}</span>
      <div className="flex-1">{children}</div>
    </div>
  );
}

function MetricRow({
  name,
  children,
}: {
  name: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-start gap-3">
      <span className="shrink-0 whitespace-nowrap rounded bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200">
        {name}
      </span>
      <p className="text-xs leading-relaxed text-white/70">{children}</p>
    </div>
  );
}
