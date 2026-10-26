/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  // TypeScript is checked explicitly in CI; keep Railway's production build
  // from failing on Next's opaque container-only typecheck phase.
  typescript: {
    ignoreBuildErrors: true,
  },
  async rewrites() {
    const backend = process.env.API_BACKEND_URL?.replace(/\/$/, "");
    return backend
      ? [{ source: "/api/:path*", destination: `${backend}/api/:path*` }]
      : [];
  },
};

export default nextConfig;

