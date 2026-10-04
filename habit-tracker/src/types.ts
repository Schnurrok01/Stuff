export type Habit = {
  id: string;
  name: string;
  emoji: string;
  color: string;
  createdAt: string;
  /** Tage, an denen die Gewohnheit erledigt wurde, als YYYY-MM-DD */
  completions: string[];
};
