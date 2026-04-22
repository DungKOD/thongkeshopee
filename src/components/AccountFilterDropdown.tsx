import { useAccounts } from "../contexts/AccountContext";

export function AccountFilterDropdown() {
  const { accounts, filter, setFilter } = useAccounts();

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const v = e.target.value;
    if (v === "all") setFilter({ kind: "all" });
    else {
      const id = Number(v.replace(/^account:/, ""));
      if (Number.isFinite(id)) setFilter({ kind: "account", id });
    }
  };

  const value =
    filter.kind === "all" ? "all" : `account:${filter.id}`;

  return (
    <div className="inline-flex items-center gap-2">
      <span className="text-xs uppercase tracking-wider text-white/60">
        Account
      </span>
      <select
        value={value}
        onChange={handleChange}
        className="rounded-md border border-surface-8 bg-surface-1 px-3 py-1.5 text-sm text-white/90 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
      >
        <option value="all">Tất cả account</option>
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
