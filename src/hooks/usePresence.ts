import { useEffect, useState } from "react";
import { auth } from "../lib/firebase";
import {
  subscribeToAllPresence,
  trackMyPresence,
  type PresenceMap,
} from "../lib/presence";
import { invoke } from "../lib/tauri";

/// Auto track presence của user đang login. Mount 1 lần ở App level —
/// re-subscribe khi auth UID đổi (logout/login).
///
/// Fetches `machine_fingerprint` từ Tauri để per-device entry trong RTDB
/// (`/status/{uid}/{fingerprint}`). 1 user nhiều máy → mỗi máy 1 entry
/// độc lập, không đè nhau.
export function useSelfPresence() {
  useEffect(() => {
    let unsubPresence: (() => void) | null = null;
    let cancelled = false;

    const subscribeForUser = async (uid: string) => {
      try {
        const fingerprint = await invoke<string>("machine_fingerprint");
        if (cancelled) return;
        unsubPresence = trackMyPresence(uid, fingerprint);
      } catch (e) {
        console.warn("[presence] fingerprint fetch failed:", e);
      }
    };

    const unsubAuth = auth.onAuthStateChanged((user) => {
      if (unsubPresence) {
        unsubPresence();
        unsubPresence = null;
      }
      if (user) {
        void subscribeForUser(user.uid);
      }
    });

    return () => {
      cancelled = true;
      unsubAuth();
      if (unsubPresence) unsubPresence();
    };
  }, []);
}

/// Admin UI hook — trả `PresenceMap` aggregated theo uid (mỗi user có
/// nhiều device entries).
///
/// **Quan trọng:** pass `enabled=false` khi không cần xem (dialog đóng) để
/// unsubscribe khỏi `/status` — giảm ~95% RTDB download bandwidth (với
/// 100 user active 24/7, tiết kiệm ~800 MB/tháng). Khi enabled=false trả `{}`.
///
/// Component dùng hook phải render qua `isUserOnline(presence[uid])` —
/// helper aggregate online từ devices + check heartbeat age. Hook tự
/// force re-render mỗi 60s để UI cập nhật derived online theo heartbeat
/// age (data RTDB không thay đổi nhưng time-based computation đổi).
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
    // Re-render mỗi 60s (khớp heartbeat interval) để recompute isOnline
    // theo heartbeat age. Nếu nhanh hơn → render lại vô ích; chậm hơn →
    // UI chuyển offline lag (trễ tới 60s sau khi user thực sự offline).
    const timer = setInterval(() => tick((x) => x + 1), 60_000);
    return () => clearInterval(timer);
  }, [enabled]);

  return statuses;
}
