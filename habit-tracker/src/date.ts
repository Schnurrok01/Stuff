const WEEKDAYS = ['So', 'Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa'];

export function toKey(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

export function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

export function todayKey(): string {
  return toKey(new Date());
}

/** Die letzten `count` Tage, ältester zuerst, heute zuletzt. */
export function lastDays(count: number): { key: string; label: string; day: number }[] {
  const today = new Date();
  return Array.from({ length: count }, (_, i) => {
    const date = addDays(today, i - count + 1);
    return { key: toKey(date), label: WEEKDAYS[date.getDay()], day: date.getDate() };
  });
}

/**
 * Aktuelle Serie: aufeinanderfolgende erledigte Tage bis heute.
 * Ist heute noch offen, zählt die Serie bis gestern weiter.
 */
export function currentStreak(completions: string[]): number {
  const done = new Set(completions);
  let cursor = new Date();
  if (!done.has(toKey(cursor))) cursor = addDays(cursor, -1);
  let streak = 0;
  while (done.has(toKey(cursor))) {
    streak++;
    cursor = addDays(cursor, -1);
  }
  return streak;
}

function fromKey(key: string): Date {
  const [y, m, d] = key.split('-').map(Number);
  return new Date(y, m - 1, d);
}

export function longestStreak(completions: string[]): number {
  const sorted = [...new Set(completions)].sort();
  let best = 0;
  let run = 0;
  let prev: string | null = null;
  for (const key of sorted) {
    run = prev && toKey(addDays(fromKey(prev), 1)) === key ? run + 1 : 1;
    best = Math.max(best, run);
    prev = key;
  }
  return best;
}

export function formatToday(): string {
  return new Date().toLocaleDateString('de-DE', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
  });
}
