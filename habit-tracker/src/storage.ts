import AsyncStorage from '@react-native-async-storage/async-storage';
import type { Habit } from './types';

const KEY = 'habits:v1';

export async function loadHabits(): Promise<Habit[]> {
  try {
    const raw = await AsyncStorage.getItem(KEY);
    return raw ? (JSON.parse(raw) as Habit[]) : [];
  } catch {
    return [];
  }
}

export async function saveHabits(habits: Habit[]): Promise<void> {
  await AsyncStorage.setItem(KEY, JSON.stringify(habits));
}
