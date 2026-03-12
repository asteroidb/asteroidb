import type { NextConfig } from "next";

const ASTEROIDB_URL =
  process.env.ASTEROIDB_URL || "http://localhost:3001";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/asteroidb/:path*",
        destination: `${ASTEROIDB_URL}/api/:path*`,
      },
      {
        source: "/healthz",
        destination: `${ASTEROIDB_URL}/healthz`,
      },
    ];
  },
};

export default nextConfig;
