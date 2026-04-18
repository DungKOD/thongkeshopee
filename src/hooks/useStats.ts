import { useCallback, useState } from "react";
import type { Day, Video } from "../types";

const uid = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : Math.random().toString(36).slice(2);

export const todayIso = () => new Date().toISOString().slice(0, 10);

export type VideoInput = Omit<Video, "id">;

export function useStats() {
  const [days, setDays] = useState<Day[]>([]);

  const addDay = useCallback(
    (date: string, videos: VideoInput[] = []) => {
      setDays((prev) => [
        {
          id: uid(),
          date,
          videos: videos.map((v) => ({ id: uid(), ...v })),
        },
        ...prev,
      ]);
    },
    [],
  );

  const removeDay = useCallback((dayId: string) => {
    setDays((prev) => prev.filter((d) => d.id !== dayId));
  }, []);

  const replaceDay = useCallback(
    (dayId: string, date: string, videos: VideoInput[]) => {
      setDays((prev) =>
        prev.map((d) =>
          d.id === dayId
            ? {
                ...d,
                date,
                videos: videos.map((v) => ({ id: uid(), ...v })),
              }
            : d,
        ),
      );
    },
    [],
  );

  const removeVideo = useCallback((dayId: string, videoId: string) => {
    setDays((prev) =>
      prev.map((d) =>
        d.id === dayId
          ? { ...d, videos: d.videos.filter((v) => v.id !== videoId) }
          : d,
      ),
    );
  }, []);

  const restoreDay = useCallback((day: Day, index: number) => {
    setDays((prev) => {
      const clamped = Math.max(0, Math.min(index, prev.length));
      return [...prev.slice(0, clamped), day, ...prev.slice(clamped)];
    });
  }, []);

  const restoreVideo = useCallback(
    (dayId: string, video: Video, index: number) => {
      setDays((prev) =>
        prev.map((d) => {
          if (d.id !== dayId) return d;
          const clamped = Math.max(0, Math.min(index, d.videos.length));
          return {
            ...d,
            videos: [
              ...d.videos.slice(0, clamped),
              video,
              ...d.videos.slice(clamped),
            ],
          };
        }),
      );
    },
    [],
  );

  return {
    days,
    addDay,
    replaceDay,
    removeDay,
    restoreDay,
    removeVideo,
    restoreVideo,
  };
}
