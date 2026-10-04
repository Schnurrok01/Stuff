import { Pressable, StyleSheet, Text, View } from 'react-native';
import * as Haptics from 'expo-haptics';
import { currentStreak, lastDays, todayKey } from '../date';
import { useTheme } from '../theme';
import type { Habit } from '../types';

type Props = {
  habit: Habit;
  onToggleDay: (dayKey: string) => void;
  onEdit: () => void;
};

export function HabitCard({ habit, onToggleDay, onEdit }: Props) {
  const theme = useTheme();
  const today = todayKey();
  const doneToday = habit.completions.includes(today);
  const streak = currentStreak(habit.completions);
  const days = lastDays(7);

  const toggle = (key: string) => {
    const willComplete = !habit.completions.includes(key);
    Haptics.impactAsync(
      willComplete ? Haptics.ImpactFeedbackStyle.Medium : Haptics.ImpactFeedbackStyle.Light,
    ).catch(() => {});
    onToggleDay(key);
  };

  return (
    <Pressable
      onLongPress={onEdit}
      delayLongPress={350}
      style={({ pressed }) => [
        styles.card,
        { backgroundColor: theme.card, opacity: pressed ? 0.9 : 1 },
      ]}
    >
      <View style={styles.row}>
        <View style={[styles.emojiBubble, { backgroundColor: habit.color + '33' }]}>
          <Text style={styles.emoji}>{habit.emoji}</Text>
        </View>
        <Pressable style={styles.info} onPress={onEdit} hitSlop={4}>
          <Text style={[styles.name, { color: theme.text }]} numberOfLines={1}>
            {habit.name}
          </Text>
          <Text style={[styles.streak, { color: theme.muted }]}>
            {streak > 0 ? `🔥 ${streak} ${streak === 1 ? 'Tag' : 'Tage'} in Folge` : 'Noch keine Serie'}
          </Text>
        </Pressable>
        <Pressable
          onPress={() => toggle(today)}
          accessibilityRole="checkbox"
          accessibilityState={{ checked: doneToday }}
          accessibilityLabel={`${habit.name} heute erledigt`}
          hitSlop={8}
          style={[
            styles.check,
            doneToday
              ? { backgroundColor: habit.color, borderColor: habit.color }
              : { borderColor: theme.border },
          ]}
        >
          {doneToday && <Text style={styles.checkMark}>✓</Text>}
        </Pressable>
      </View>

      <View style={styles.week}>
        {days.map((d) => {
          const done = habit.completions.includes(d.key);
          const isToday = d.key === today;
          return (
            <Pressable
              key={d.key}
              onPress={() => toggle(d.key)}
              style={styles.dayCol}
              hitSlop={4}
              accessibilityLabel={`${d.label} ${d.day}, ${done ? 'erledigt' : 'offen'}`}
            >
              <Text style={[styles.dayLabel, { color: isToday ? theme.text : theme.muted }]}>
                {d.label}
              </Text>
              <View
                style={[
                  styles.dot,
                  { backgroundColor: done ? habit.color : theme.empty },
                  isToday && !done && { borderWidth: 2, borderColor: habit.color },
                ]}
              >
                <Text style={[styles.dayNum, { color: done ? '#fff' : theme.muted }]}>{d.day}</Text>
              </View>
            </Pressable>
          );
        })}
      </View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 18,
    padding: 16,
    marginBottom: 12,
    gap: 14,
  },
  row: { flexDirection: 'row', alignItems: 'center', gap: 12 },
  emojiBubble: {
    width: 46,
    height: 46,
    borderRadius: 23,
    alignItems: 'center',
    justifyContent: 'center',
  },
  emoji: { fontSize: 24 },
  info: { flex: 1 },
  name: { fontSize: 17, fontWeight: '600' },
  streak: { fontSize: 13, marginTop: 2 },
  check: {
    width: 38,
    height: 38,
    borderRadius: 19,
    borderWidth: 2,
    alignItems: 'center',
    justifyContent: 'center',
  },
  checkMark: { color: '#fff', fontSize: 20, fontWeight: '700' },
  week: { flexDirection: 'row', justifyContent: 'space-between' },
  dayCol: { alignItems: 'center', gap: 4 },
  dayLabel: { fontSize: 11, fontWeight: '500' },
  dot: {
    width: 32,
    height: 32,
    borderRadius: 16,
    alignItems: 'center',
    justifyContent: 'center',
  },
  dayNum: { fontSize: 12, fontWeight: '600' },
});
