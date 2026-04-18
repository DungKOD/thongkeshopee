---
paths:
  - "src/**/*.{ts,tsx}"
---

# React Frontend Rules

- Props interface: `[Component]Props`
- Hooks return object, không return array
- Loading states: dùng skeleton component
- Empty states: luôn có empty state UI
- Tauri invoke wrapper: đặt trong `src/lib/tauri.ts`
- Test file: `__tests__/[Component].test.tsx` cùng thư mục
