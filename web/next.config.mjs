/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  async rewrites() {
    const backend = process.env.API_BACKEND_URL?.replace(/\/$/, "");
    return backend
      ? [{ source: "/api/:path*", destination: `${backend}/api/:path*` }]
      : [];
  },
};

export default nextConfig;

