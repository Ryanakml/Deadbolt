export function resolveInitialTheme(storedTheme, prefersDark) {
    if (storedTheme === "dark" || storedTheme === "light")
        return storedTheme;
    return prefersDark ? "dark" : "light";
}
export function applyDashboardTheme(root, toggle, theme) {
    const isDark = theme === "dark";
    root.classList.toggle("dark", isDark);
    root.classList.toggle("light", !isDark);
    toggle.setAttribute("aria-pressed", isDark ? "true" : "false");
}
//# sourceMappingURL=theme.js.map