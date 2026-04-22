module.exports = {
  content: ["./django_app/templates/**/*.html"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        primary: "#6debfd",
        secondary: "#ff7348",
        background: "#070f12",
        surface: "#070f12",
        "surface-container": "#111b1f",
        "surface-container-high": "#172125",
        "surface-container-highest": "#1c272c",
        "surface-variant": "#1c272c",
        "on-surface": "#f0f8fc",
        "on-surface-variant": "#a4acb0",
        "on-primary-container": "#00272c",
        "on-secondary-container": "#ffe6df",
        "primary-container": "#18b1c1",
        "secondary-container": "#a12d01",
        outline: "#6e777a",
        "outline-variant": "#41494d",
      },
      fontFamily: {
        headline: ["ui-sans-serif", "system-ui", "sans-serif"],
        body: ["ui-sans-serif", "system-ui", "sans-serif"],
        label: ["ui-sans-serif", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
