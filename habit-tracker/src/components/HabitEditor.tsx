import { useEffect, useState } from 'react';
import {
  Alert,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import { longestStreak } from '../date';
import { HABIT_COLORS, HABIT_EMOJIS, useTheme } from '../theme';
import type { Habit } from '../types';
import type { HabitInput } from '../useHabits';

type Props = {
  visible: boolean;
  /** Vorhandene Gewohnheit zum Bearbeiten, sonst wird eine neue angelegt */
  habit: Habit | null;
  onClose: () => void;
  onSave: (input: HabitInput) => void;
  onDelete: () => void;
};

export function HabitEditor({ visible, habit, onClose, onSave, onDelete }: Props) {
  const theme = useTheme();
  const [name, setName] = useState('');
  const [emoji, setEmoji] = useState(HABIT_EMOJIS[0]);
  const [color, setColor] = useState(HABIT_COLORS[5]);

  useEffect(() => {
    if (!visible) return;
    setName(habit?.name ?? '');
    setEmoji(habit?.emoji ?? HABIT_EMOJIS[0]);
    setColor(habit?.color ?? HABIT_COLORS[5]);
  }, [visible, habit]);

  const canSave = name.trim().length > 0;

  const save = () => {
    if (!canSave) return;
    onSave({ name: name.trim(), emoji, color });
  };

  const confirmDelete = () => {
    Alert.alert('Gewohnheit löschen?', `„${habit?.name}" und der gesamte Verlauf werden entfernt.`, [
      { text: 'Abbrechen', style: 'cancel' },
      { text: 'Löschen', style: 'destructive', onPress: onDelete },
    ]);
  };

  return (
    <Modal
      visible={visible}
      animationType="slide"
      presentationStyle="pageSheet"
      onRequestClose={onClose}
    >
      <View style={[styles.sheet, { backgroundColor: theme.background }]}>
        <View style={[styles.header, { borderBottomColor: theme.border }]}>
          <Pressable onPress={onClose} hitSlop={10}>
            <Text style={[styles.headerButton, { color: theme.accent }]}>Abbrechen</Text>
          </Pressable>
          <Text style={[styles.title, { color: theme.text }]}>
            {habit ? 'Bearbeiten' : 'Neue Gewohnheit'}
          </Text>
          <Pressable onPress={save} disabled={!canSave} hitSlop={10}>
            <Text
              style={[
                styles.headerButton,
                styles.bold,
                { color: canSave ? theme.accent : theme.muted },
              ]}
            >
              Sichern
            </Text>
          </Pressable>
        </View>

        <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
          <View style={[styles.preview, { backgroundColor: color + '33' }]}>
            <Text style={styles.previewEmoji}>{emoji}</Text>
          </View>

          <TextInput
            value={name}
            onChangeText={setName}
            placeholder="z. B. 2 Liter Wasser trinken"
            placeholderTextColor={theme.muted}
            style={[styles.input, { backgroundColor: theme.card, color: theme.text }]}
            autoFocus={!habit}
            returnKeyType="done"
            onSubmitEditing={save}
            maxLength={40}
          />

          <Text style={[styles.section, { color: theme.muted }]}>SYMBOL</Text>
          <View style={[styles.grid, { backgroundColor: theme.card }]}>
            {HABIT_EMOJIS.map((e) => (
              <Pressable
                key={e}
                onPress={() => setEmoji(e)}
                style={[styles.option, e === emoji && { backgroundColor: color + '44' }]}
              >
                <Text style={styles.optionEmoji}>{e}</Text>
              </Pressable>
            ))}
          </View>

          <Text style={[styles.section, { color: theme.muted }]}>FARBE</Text>
          <View style={[styles.grid, { backgroundColor: theme.card }]}>
            {HABIT_COLORS.map((c) => (
              <Pressable
                key={c}
                onPress={() => setColor(c)}
                style={[
                  styles.swatch,
                  { backgroundColor: c },
                  c === color && { borderWidth: 3, borderColor: theme.text },
                ]}
              />
            ))}
          </View>

          {habit && (
            <>
              <Text style={[styles.section, { color: theme.muted }]}>STATISTIK</Text>
              <View style={[styles.stats, { backgroundColor: theme.card }]}>
                <Stat label="Insgesamt erledigt" value={habit.completions.length} />
                <Stat label="Längste Serie" value={longestStreak(habit.completions)} />
              </View>

              <Pressable
                onPress={confirmDelete}
                style={[styles.deleteButton, { backgroundColor: theme.card }]}
              >
                <Text style={[styles.deleteText, { color: theme.danger }]}>Gewohnheit löschen</Text>
              </Pressable>
            </>
          )}
        </ScrollView>
      </View>
    </Modal>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  const theme = useTheme();
  return (
    <View style={styles.stat}>
      <Text style={[styles.statValue, { color: theme.text }]}>{value}</Text>
      <Text style={[styles.statLabel, { color: theme.muted }]}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  sheet: { flex: 1 },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  headerButton: { fontSize: 17 },
  bold: { fontWeight: '600' },
  title: { fontSize: 17, fontWeight: '600' },
  content: { padding: 16, paddingBottom: 48 },
  preview: {
    alignSelf: 'center',
    width: 88,
    height: 88,
    borderRadius: 44,
    alignItems: 'center',
    justifyContent: 'center',
    marginVertical: 12,
  },
  previewEmoji: { fontSize: 44 },
  input: {
    fontSize: 17,
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderRadius: 12,
  },
  section: {
    fontSize: 13,
    marginTop: 24,
    marginBottom: 8,
    marginLeft: 4,
  },
  grid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'space-between',
    rowGap: 8,
    padding: 12,
    borderRadius: 12,
  },
  option: {
    width: '11.5%',
    aspectRatio: 1,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center',
  },
  optionEmoji: { fontSize: 24 },
  swatch: { width: 34, height: 34, borderRadius: 17 },
  stats: { flexDirection: 'row', borderRadius: 12, paddingVertical: 16 },
  stat: { flex: 1, alignItems: 'center' },
  statValue: { fontSize: 28, fontWeight: '700' },
  statLabel: { fontSize: 13, marginTop: 2 },
  deleteButton: {
    marginTop: 24,
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: 'center',
  },
  deleteText: { fontSize: 17 },
});
