import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "TransitScope",
  description: "A live assessment of potential exoplanet candidates.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

