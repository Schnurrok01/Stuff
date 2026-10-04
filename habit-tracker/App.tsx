import { useEffect, useState } from 'react';
import { AppState, FlatList, Pressable, StyleSheet, Text, View } from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { SafeAreaProvider, useSafeAreaInsets } from 'react-native-safe-area-context';
import { HabitCard } from './src/components/HabitCard';
import { HabitEditor } from './src/components/HabitEditor';
import { formatToday, todayKey } from './src/date';
import { useTheme } from './src/theme';
import { useHabits } from './src/useHabits';

export default function App() {
  return (
    <SafeAreaProvider>
      <HomeScreen />
    </SafeAreaProvider>
  );
}

function HomeScreen() {
  const theme = useTheme();
  const insets = useSafeAreaInsets();
  const { habits, loaded, addHabit, updateHabit, deleteHabit, toggleDay } = useHabits();
  const [editorOpen, setEditorOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [, setRefresh] = useState(0);

  // Beim Zurückkehren in die App neu rendern, damit „heute" nach Mitternacht stimmt
  useEffect(() => {
    const sub = AppState.addEventListener('change', (state) => {
      if (state === 'active') setRefresh((n) => n + 1);
    });
    return () => sub.remove();
  }, []);

  const editing = habits.find((h) => h.id === editingId) ?? null;
  const today = todayKey();
  const doneCount = habits.filter((h) => h.completions.includes(today)).length;
  const progress = habits.length ? doneCount / habits.length : 0;

  const openNew = () => {
    setEditingId(null);
    setEditorOpen(true);
  };

  const openEdit = (id: string) => {
    setEditingId(id);
    setEditorOpen(true);
  };

  const closeEditor = () => setEditorOpen(false);

  return (
    <View style={[styles.container, { backgroundColor: theme.background }]}>
      <StatusBar style="auto" />
      <FlatList
        data={habits}
        keyExtractor={(h) => h.id}
        contentContainerStyle={{
          paddingTop: insets.top + 12,
          paddingBottom: insets.bottom + 100,
          paddingHorizontal: 16,
        }}
        ListHeaderComponent={
          <View style={styles.header}>
            <Text style={[styles.date, { color: theme.muted }]}>{formatToday()}</Text>
            <Text style={[styles.title, { color: theme.text }]}>Gewohnheiten</Text>
            {habits.length > 0 && (
              <View style={[styles.summary, { backgroundColor: theme.card }]}>
                <View style={styles.summaryRow}>
                  <Text style={[styles.summaryText, { color: theme.text }]}>
                    {doneCount === habits.length
                      ? 'Alles erledigt – stark! 🎉'
                      : `${doneCount} von ${habits.length} heute erledigt`}
                  </Text>
                  <Text style={[styles.summaryPercent, { color: theme.accent }]}>
                    {Math.round(progress * 100)} %
                  </Text>
                </View>
                <View style={[styles.track, { backgroundColor: theme.empty }]}>
                  <View
                    style={[
                      styles.fill,
                      { width: `${progress * 100}%`, backgroundColor: theme.accent },
                    ]}
                  />
                </View>
              </View>
            )}
          </View>
        }
        ListEmptyComponent={
          loaded ? (
            <View style={styles.empty}>
              <Text style={styles.emptyEmoji}>🌱</Text>
              <Text style={[styles.emptyTitle, { color: theme.text }]}>Noch keine Gewohnheiten</Text>
              <Text style={[styles.emptyText, { color: theme.muted }]}>
                Tippe auf „+", um deine erste Gewohnheit anzulegen.
              </Text>
            </View>
          ) : null
        }
        renderItem={({ item }) => (
          <HabitCard
            habit={item}
            onToggleDay={(day) => toggleDay(item.id, day)}
            onEdit={() => openEdit(item.id)}
          />
        )}
        ListFooterComponent={
          habits.length > 0 ? (
            <Text style={[styles.hint, { color: theme.muted }]}>
              Tippe auf einen Tag, um ihn nachzutragen. Tippe auf den Namen zum Bearbeiten.
            </Text>
          ) : null
        }
      />

      <Pressable
        onPress={openNew}
        accessibilityLabel="Neue Gewohnheit"
        style={({ pressed }) => [
          styles.fab,
          {
            backgroundColor: theme.accent,
            bottom: insets.bottom + 24,
            transform: [{ scale: pressed ? 0.94 : 1 }],
          },
        ]}
      >
        <Text style={styles.fabText}>+</Text>
      </Pressable>

      <HabitEditor
        visible={editorOpen}
        habit={editing}
        onClose={closeEditor}
        onSave={(input) => {
          if (editing) updateHabit(editing.id, input);
          else addHabit(input);
          closeEditor();
        }}
        onDelete={() => {
          if (editing) deleteHabit(editing.id);
          closeEditor();
        }}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { marginBottom: 16 },
  date: { fontSize: 13, fontWeight: '600', textTransform: 'uppercase' },
  title: { fontSize: 34, fontWeight: '800', marginTop: 2 },
  summary: { borderRadius: 18, padding: 16, marginTop: 16, gap: 10 },
  summaryRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  summaryText: { fontSize: 15, fontWeight: '600' },
  summaryPercent: { fontSize: 15, fontWeight: '700' },
  track: { height: 8, borderRadius: 4, overflow: 'hidden' },
  fill: { height: '100%', borderRadius: 4 },
  empty: { alignItems: 'center', marginTop: 80, paddingHorizontal: 32 },
  emptyEmoji: { fontSize: 56 },
  emptyTitle: { fontSize: 20, fontWeight: '700', marginTop: 12 },
  emptyText: { fontSize: 15, textAlign: 'center', marginTop: 6 },
  hint: { fontSize: 12, textAlign: 'center', marginTop: 4 },
  fab: {
    position: 'absolute',
    right: 24,
    width: 60,
    height: 60,
    borderRadius: 30,
    alignItems: 'center',
    justifyContent: 'center',
    shadowColor: '#000',
    shadowOpacity: 0.25,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 4 },
  },
  fabText: { color: '#fff', fontSize: 32, fontWeight: '400', marginTop: -2 },
});
