/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Inter",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
      },
      colors: {
        shopee: {
          50: "#FFF4F1",
          100: "#FFE4DC",
          200: "#FFC2B0",
          300: "#FFA085",
          400: "#F5744E",
          500: "#EE4D2D",
          600: "#D73B1C",
          700: "#B8301A",
          800: "#8F2514",
          900: "#661A0E",
        },
        // Material Design dark surfaces (base + tint overlay đã baked-in).
        surface: {
          0: "#121212", // base
          1: "#1E1E1E", // 1dp elevation
          2: "#222222", // 2dp
          3: "#242424", // 3dp
          4: "#272727", // 4dp
          6: "#2C2C2C", // 6dp
          8: "#2E2E2E", // 8dp
          12: "#333333",
          16: "#353535",
          24: "#383838",
        },
      },
      boxShadow: {
        // Material Design dark elevation shadows
        "elev-1":
          "0 1px 1px 0 rgba(0,0,0,0.14), 0 2px 1px -1px rgba(0,0,0,0.12), 0 1px 3px 0 rgba(0,0,0,0.20)",
        "elev-2":
          "0 2px 2px 0 rgba(0,0,0,0.14), 0 3px 1px -2px rgba(0,0,0,0.12), 0 1px 5px 0 rgba(0,0,0,0.20)",
        "elev-4":
          "0 4px 5px 0 rgba(0,0,0,0.14), 0 1px 10px 0 rgba(0,0,0,0.12), 0 2px 4px -1px rgba(0,0,0,0.20)",
        "elev-8":
          "0 8px 10px 1px rgba(0,0,0,0.14), 0 3px 14px 2px rgba(0,0,0,0.12), 0 5px 5px -3px rgba(0,0,0,0.20)",
        "elev-16":
          "0 16px 24px 2px rgba(0,0,0,0.14), 0 6px 30px 5px rgba(0,0,0,0.12), 0 8px 10px -5px rgba(0,0,0,0.20)",
        "elev-24":
          "0 24px 38px 3px rgba(0,0,0,0.14), 0 9px 46px 8px rgba(0,0,0,0.12), 0 11px 15px -7px rgba(0,0,0,0.20)",
      },
      borderRadius: {
        md: "8px",
        lg: "12px",
        xl: "16px",
        "2xl": "24px",
      },
    },
  },
  plugins: [],
};
