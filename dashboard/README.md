# TGF Parkrun Dashboard

The frontend component of the Parkrun Data Dive. This is an **Astro SSR** application optimized for mobile devices and secure data handling.

## 🛠 Technical Implementation

- **SSR Mode:** The application runs in server mode (Node/Firebase) to keep BigQuery credentials and raw data off the client side.
- **View-Based Data Layer:** All dashboard components query published BigQuery views (named `_NN_dashboard_*`) instead of embedding complex SQL. Views precompute aggregations, rankings, and transformations server-side. Components execute lightweight `SELECT *` queries via the `runQuery` utility, then format/filter results for display.
- **Global Styling:** All baseline resets and design tokens (colors/typography) are centralized in `Layout.astro` using `is:global`.

## 🚀 Key Features

- **Latest Run Report:** A comprehensive breakdown of the most recent event including finish time distribution (ApexCharts), first finishers, and top age-grade performances.
  - **Weather Context:** Reads cached historical weather data (temp, wind, conditions) from BigQuery `event_weather` (synced via utility script).
  - **Trend Analysis:** Automated comparison of finishers, volunteers, and PBs against the previous event.
  - **Archive Navigation:** Full pagination support to view reports for any past event.
- **Volunteer Milestone Tracker:** Identification of volunteers approaching major milestones (10, 25, 50, 100, 250, 500).
  - **Hybrid Data:** Combines local event-specific volunteer history with global `vol_count` data from the results table for maximum accuracy.
  - **Interactive Table:** Supports real-time client-side filtering (name/ID) and multi-column sorting.
- **Visitors Map:** A geographic visualization of where athletes travel from.
  - **Cached Coordinates:** Uses `_22_dashboard_visitor_stats` with coordinates resolved server-side via `event_coordinates` in BigQuery.
  - **Heatmap Layer:** Toggleable density overlay to see "hot spots" of visitor origin.
  - **Smart Preview:** homepage widget provides a static, zero-interaction snapshot for public viewing, while the full interactive version remains protected.
- **Course Records:** Deep-dive into the fastest times ever recorded at the event, filterable by age category and gender.

## Key Directories

- `src/layouts/`: The `Layout.astro` component wraps all pages with a 1024px max-width container and global styles.
- `src/components/`: Each dashboard component queries its corresponding published BigQuery view:
  - `HeadlineStats.astro` → `_20_dashboard_headline_stats`: Aggregate parkrun statistics (events, finishers, distance, PBs).
  - `RunReport.astro` → `_26_dashboard_run_report`: Comprehensive breakdown of the latest event including weather, trends, and nested detail arrays.
  - `VolunteerMilestones.astro` → `_23_dashboard_volunteer_milestones`: Milestone progress tracker for volunteers approaching 10, 25, 50, 100, 250, 500 events.
  - `Visitors Map / HomeRunMap.astro` → `_22_dashboard_visitor_stats`: Geographic visualization of where athletes travel from.
  - `Records.astro` → `_21_dashboard_course_records`: Top 10 personal-best runners by category/gender.
  - `AttendanceTracker.astro` → `_24_dashboard_attendance_tracker`: Timeline/comparison charts by gender and age group.
  - `PerformanceTracker.astro` → `_25_dashboard_performance_tracker`: Race-time trends by date with gender/age filters.
  - `TopLists.astro` → `_27_dashboard_top_lists`: Six independent top-20 leaderboards (athletes, volunteers, events, clubs).
  - `VolunteerTracker.astro` → `_28_dashboard_volunteer_tracker`: Weekly volunteer-support timeline and role breakdowns.
  - `Header.astro`: Responsive navigation with animated hamburger-to-X SVG logic.
- `src/lib/`: Backend utilities including BigQuery authentication (ADC compatible) and `runQuery` helper.

## 🔐 Security & Privacy

Athlete names are personally identifiable information (PII). This dashboard follows these rules:

1. No JSON data files are ever committed to the repo.
2. Data is fetched on the server; the client only receives rendered HTML.
3. Key normalization ensures that BigQuery's case-sensitive column names are handled gracefully in JavaScript.

## Display Conventions

- Dates shown in dashboard UI should use `dd-mm-yyyy` format for consistency across pages.
- If using `toLocaleDateString('en-GB')`, normalize separators to hyphens with `.split('/').join('-')`.

## 🧞 Commands

All commands are run from the `dashboard/` folder:

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
