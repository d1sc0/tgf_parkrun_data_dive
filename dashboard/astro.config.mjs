import { defineConfig } from 'astro/config';
import node from '@astrojs/node';

export default defineConfig({
  site: 'https://tgf-parkrun.hellostu.xyz',
  output: 'server',
  adapter: node({
    mode: 'standalone',
  }),
});
