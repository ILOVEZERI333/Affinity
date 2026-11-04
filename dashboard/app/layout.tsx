export const metadata = { title: "Distriscrape" };

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui", margin: 24, maxWidth: 960, colorScheme: "light dark" }}>{children}</body>
    </html>
  );
}
