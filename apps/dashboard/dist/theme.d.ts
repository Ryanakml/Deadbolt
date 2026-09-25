export type DashboardTheme = "light" | "dark";
interface ThemeRoot {
    classList: {
        toggle(name: string, force?: boolean): boolean;
    };
}
interface ThemeToggle {
    setAttribute(name: string, value: string): void;
}
export declare function resolveInitialTheme(storedTheme: string | null, prefersDark: boolean): DashboardTheme;
export declare function applyDashboardTheme(root: ThemeRoot, toggle: ThemeToggle, theme: DashboardTheme): void;
export {};
//# sourceMappingURL=theme.d.ts.map