import { useAccounts } from "../contexts/AccountContext";

export function AccountFilterDropdown() {
  const { accounts, filter, setFilter } = useAccounts();

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const v = e.target.value;
    if (v === "all") setFilter({ kind: "all" });
    else {
      // id là string (content_id hash có thể > 2^53) — strip prefix thôi.
      const id = v.replace(/^account:/, "");
      if (id) setFilter({ kind: "account", id });
    }
  };

  const value =
    filter.kind === "all" ? "all" : `account:${filter.id}`;

  return (
    <div className="inline-flex items-center gap-1.5">
      <span
        className="material-symbols-rounded shrink-0 text-base text-shopee-400"
        title="Tài khoản Shopee"
      >
        store
      </span>
      <select
        value={value}
        onChange={handleChange}
        title="Lọc theo tài khoản Shopee"
        className="max-w-[120px] truncate rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-sm text-white/90 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
      >
        <option value="all">Tất cả</option>
        {(accounts ?? []).map((a) => (
          <option key={a.id} value={`account:${a.id}`}>
            {a.name}
            {a.rowCount > 0 ? ` (${a.rowCount})` : ""}
          </option>
        ))}
      </select>
    </div>
  );
}
