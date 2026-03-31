# TGF Parkrun Dashboard

The frontend component of the Parkrun Data Dive. This is an **Astro SSR** application optimized for mobile devices and secure data handling.

## 🛠 Technical Implementation

- **SSR Mode:** The application runs in server mode (Node/Firebase) to keep BigQuery credentials and raw data off the client side.
- ** BigQuery SQL** queries are defined as string literals directly within the Astro components, referencing environment variables for project and dataset IDs. They are executed via a `runQuery` utility.
- **Global Styling:** All baseline resets and design tokens (colors/typography) are centralized in `Layout.astro` using `is:global`.

## 🚀 Key Features

- **Latest Run Report:** A comprehensive breakdown of the most recent event including finish time distribution (ApexCharts), first finishers, and top age-grade performances.
  - **Weather Context:** Integrates historical weather data (temp, wind, conditions) from the Open-Meteo API at the start time of each event.
  - **Trend Analysis:** Automated comparison of finishers, volunteers, and PBs against the previous event.
  - **Archive Navigation:** Full pagination support to view reports for any past event.
- **Volunteer Milestone Tracker:** Identification of volunteers approaching major milestones (10, 25, 50, 100, 250, 500).
  - **Hybrid Data:** Combines local event-specific volunteer history with global `vol_count` data from the results table for maximum accuracy.
  - **Interactive Table:** Supports real-time client-side filtering (name/ID) and multi-column sorting.
- **Visitors Map:** A geographic visualization of where athletes travel from.
  - **Heatmap Layer:** Toggleable density overlay to see "hot spots" of visitor origin.
  - **Smart Preview:** homepage widget provides a static, zero-interaction snapshot for public viewing, while the full interactive version remains protected.
- **Course Records:** Deep-dive into the fastest times ever recorded at the event, filterable by age category and gender.

## � Key Directories

- `src/layouts/`: The `Layout.astro` component wraps all pages with a 1024px max-width container and global styles.
- `src/components/`:
  - `HeadlineStats.astro`: A "Smart Widget" that handles its own BigQuery data fetching and key normalization.
  - `RunReport.astro`: Complex reporter component using BigQuery window functions for historical navigation.
  - `VolunteerMilestones.astro`: Mobile-optimized tracker using a card-based layout on small screens.
  - `HomeRunMap.astro`: Leaflet-based mapping component with `ResizeObserver` for layout stability.
  - `Header.astro`: Contains the responsive navigation and animated hamburger-to-X SVG logic.
- `src/lib/`: Backend utilities for BigQuery authentication (ADC compatible).

## 🔐 Security & Privacy

Athlete names are personally identifiable information (PII). This dashboard follows these rules:

1. No JSON data files are ever committed to the repo.
2. Data is fetched on the server; the client only receives rendered HTML.
3. Key normalization ensures that BigQuery's case-sensitive column names are handled gracefully in JavaScript.

```

Astro looks for `.astro` or `.md` files in the `src/pages/` directory. Each page is exposed as a route based on its file name.

There's nothing special about `src/components/`, but that's where we like to put any Astro/React/Vue/Svelte/Preact components.

Any static assets, like images, can be placed in the `public/` directory.

## 🧞 Commands

All commands are run from the root of the project, from a terminal:

| Command                   | Action                                           |
| :------------------------ | :----------------------------------------------- |
| `npm install`             | Installs dependencies                            |
| `npm run dev`             | Starts local dev server at `localhost:4321`      |
| `npm run build`           | Build your production site to `./dist/`          |
| `npm run preview`         | Preview your build locally, before deploying     |
| `npm run astro ...`       | Run CLI commands like `astro add`, `astro check` |
| `npm run astro -- --help` | Get help using the Astro CLI                     |

## 👀 Want to learn more?

Feel free to check [our documentation](https://docs.astro.build) or jump into our [Discord server](https://astro.build/chat).
```
