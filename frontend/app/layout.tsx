import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Echo - AI Data Scientist",
  description: "Transform messy business data into clear insights. AI-powered analytics for small businesses.",
  keywords: ["analytics", "AI", "data science", "business intelligence", "metrics", "reporting"],
  authors: [{ name: "Echo Team" }],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-[#0f172a] text-slate-50`}
      >
        {children}
      </body>
    </html>
  );
}
