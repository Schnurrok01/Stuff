import { useCallback, useEffect, useRef, useState } from 'react';
import { loadHabits, saveHabits } from './storage';
import type { Habit } from './types';

export type HabitInput = Pick<Habit, 'name' | 'emoji' | 'color'>;

export function useHabits() {
  const [habits, setHabits] = useState<Habit[]>([]);
  const [loaded, setLoaded] = useState(false);
  const skipSave = useRef(true);

  useEffect(() => {
    loadHabits().then((stored) => {
      setHabits(stored);
      setLoaded(true);
    });
  }, []);

  useEffect(() => {
    // Den ersten Durchlauf nach dem Laden nicht zurückschreiben
    if (!loaded) return;
    if (skipSave.current) {
      skipSave.current = false;
      return;
    }
    saveHabits(habits).catch(() => {});
  }, [habits, loaded]);

  const addHabit = useCallback((input: HabitInput) => {
    const habit: Habit = {
      ...input,
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      completions: [],
    };
    setHabits((prev) => [...prev, habit]);
  }, []);

  const updateHabit = useCallback((id: string, input: HabitInput) => {
    setHabits((prev) => prev.map((h) => (h.id === id ? { ...h, ...input } : h)));
  }, []);

  const deleteHabit = useCallback((id: string) => {
    setHabits((prev) => prev.filter((h) => h.id !== id));
  }, []);

  const toggleDay = useCallback((id: string, dayKey: string) => {
    setHabits((prev) =>
      prev.map((h) => {
        if (h.id !== id) return h;
        const done = h.completions.includes(dayKey);
        return {
          ...h,
          completions: done
            ? h.completions.filter((d) => d !== dayKey)
            : [...h.completions, dayKey],
        };
      }),
    );
  }, []);

  return { habits, loaded, addHabit, updateHabit, deleteHabit, toggleDay };
}
