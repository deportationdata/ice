library(dplyr)
library(lubridate)
library(arrow)
library(ggplot2)
library(stringr)

# --- build weekly table with source file labels (string) ---
removals_data <- read_feather("./data/ice-processed/removals-merged-with-flags.feather") |>
  mutate(Departed_Date = as.Date(Departed_Date)) |>
  filter(drop_row == FALSE)

weekly_counts <- removals_data |>
  filter(!is.na(Departed_Date)) |>
  mutate(
    week_start = floor_date(Departed_Date, unit = "week", week_start = 7) # Sunday
  ) |>
  group_by(week_start) |>
  summarise(
    n = n(),
    source_files = paste(sort(unique(source_file)), collapse = ", "),
    .groups = "drop"
  ) |>
  arrange(week_start) |>
  mutate(
    source_label = str_wrap(source_files, width = 35),
    changed = source_files != dplyr::lag(source_files, default = first(source_files))
  )

# --- create segments where source_files is constant ---
segments <- weekly_counts |>
  mutate(seg_id = cumsum(changed)) |>
  group_by(seg_id) |>
  summarise(
    seg_start = min(week_start),
    seg_end   = max(week_start),
    seg_mid   = seg_start + as.numeric(seg_end - seg_start) / 2,
    seg_label = dplyr::first(source_label),
    .groups = "drop"
  )

# vertical lines at starts of new segments (excluding first)
vlines <- segments |>
  arrange(seg_start) |>
  slice(-1)

# --- plot (JUST THIS PLOT) ---
p <- ggplot(weekly_counts, aes(x = week_start, y = n)) +
  geom_line() +
  geom_vline(
    data = vlines,
    aes(xintercept = as.numeric(seg_start)),
    linetype = "dashed",
    alpha = 0.7
  ) +
  geom_label(
    data = segments,
    aes(x = seg_mid, y = Inf, label = seg_label),
    vjust = 1.15,
    label.size = 0.2,
    alpha = 0.9,
    inherit.aes = FALSE
  ) +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.18))) +
  labs(
    title = "Weekly Removals with Data Source Segments",
    x = "Week start (Sunday)",
    y = "Number of removals (n)"
  ) +
  theme_minimal()

p

# Optional: save for the meeting
ggsave("removals_weekly_by_source.png", p, width = 12, height = 6, dpi = 300)