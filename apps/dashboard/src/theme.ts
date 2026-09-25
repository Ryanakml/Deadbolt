export type DashboardTheme = "light" | "dark";

interface ThemeRoot {
  classList: {
    toggle(name: string, force?: boolean): boolean;
  };
}

interface ThemeToggle {
  setAttribute(name: string, value: string): void;
}

export function resolveInitialTheme(
  storedTheme: string | null,
  prefersDark: boolean,
): DashboardTheme {
  if (storedTheme === "dark" || storedTheme === "light") return storedTheme;
  return prefersDark ? "dark" : "light";
}

export function applyDashboardTheme(
  root: ThemeRoot,
  toggle: ThemeToggle,
  theme: DashboardTheme,
): void {
  const isDark = theme === "dark";
  root.classList.toggle("dark", isDark);
  root.classList.toggle("light", !isDark);
  toggle.setAttribute("aria-pressed", isDark ? "true" : "false");
}
