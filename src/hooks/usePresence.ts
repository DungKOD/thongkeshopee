import { useEffect, useState } from "react";
import { auth } from "../lib/firebase";
import {
  subscribeToAllPresence,
  trackMyPresence,
  type PresenceMap,
} from "../lib/presence";

/// Tự động track presence của user đang login. Mount 1 lần ở App level —
/// re-subscribe khi auth UID đổi (logout/login).
export function useSelfPresence() {
  useEffect(() => {
    let unsubPresence: (() => void) | null = null;
    const unsubAuth = auth.onAuthStateChanged((user) => {
      if (unsubPresence) {
        unsubPresence();
        unsubPresence = null;
      }
      if (user) {
        unsubPresence = trackMyPresence(user.uid);
      }
    });
    return () => {
      unsubAuth();
      if (unsubPresence) unsubPresence();
    };
  }, []);
}

/// Admin UI hook — trả map `{uid → Presence}` subscribe realtime.
///
/// **Quan trọng:** pass `enabled=false` khi không cần xem (dialog đóng) để
/// unsubscribe khỏi `/status` — giảm 95% RTDB download bandwidth (với 100
/// user active 24/7, tiết kiệm ~800MB/tháng). Khi enabled=false trả `{}`.
///
/// Component dùng hook này cần render qua `isOnline(presence)` (không trust
/// field `state` thẳng). Hook tự force re-render mỗi 30s để UI cập nhật
/// derived online status theo heartbeat age (data RTDB không thay đổi nhưng
/// time-based computation đổi theo thời gian).
export function useAllPresence(enabled: boolean = true): PresenceMap {
  const [statuses, setStatuses] = useState<PresenceMap>({});
  const [, tick] = useState(0);

  useEffect(() => {
    if (!enabled) {
      setStatuses({});
      return;
    }
    return subscribeToAllPresence(setStatuses);
  }, [enabled]);

  useEffect(() => {
    if (!enabled) return;
    // Re-render mỗi 60s (khớp heartbeat interval) để recompute isOnline theo
    // heartbeat age. Nếu nhanh hơn sẽ render lại vô ích; chậm hơn thì UI
    // chuyển offline lag.
    const timer = setInterval(() => tick((x) => x + 1), 60_000);
    return () => clearInterval(timer);
  }, [enabled]);

  return statuses;
}
