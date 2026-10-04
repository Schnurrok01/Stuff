import { useColorScheme } from 'react-native';

export const HABIT_COLORS = [
  '#FF6B6B',
  '#FF9F43',
  '#FECA57',
  '#1DD1A1',
  '#48DBFB',
  '#54A0FF',
  '#5F27CD',
  '#FF9FF3',
];

export const HABIT_EMOJIS = [
  '💧', '🏃', '📚', '🧘', '🥗', '😴', '💪', '🚭',
  '✍️', '🎸', '🧹', '💊', '🚶', '🦷', '☀️', '🙏',
];

const light = {
  background: '#F2F2F7',
  card: '#FFFFFF',
  text: '#1C1C1E',
  muted: '#8E8E93',
  border: '#E5E5EA',
  empty: '#E9E9EE',
  danger: '#FF3B30',
  accent: '#007AFF',
};

const dark: typeof light = {
  background: '#000000',
  card: '#1C1C1E',
  text: '#F2F2F7',
  muted: '#8E8E93',
  border: '#38383A',
  empty: '#2C2C2E',
  danger: '#FF453A',
  accent: '#0A84FF',
};

export type Theme = typeof light;

export function useTheme(): Theme {
  return useColorScheme() === 'dark' ? dark : light;
}
