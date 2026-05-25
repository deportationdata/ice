library(data.table)
library(dplyr)
library(tibble)

merge_boundary_crossers <- function(older, newer, boundary,
                                    bi_col = "Detention_Book_In_Date",
                                    bo_col = "Detention_Book_Out_Date",
                                    fac_col = "Detention_Facility_Code",
                                    gender_col = "Gender",
                                    citizen_col = "Citizenship_Country",
                                    extra_match_cols = character(),
                                    drift_days = 7L,
                                    block_merge_equal_n = FALSE,
                                    label = "boundary") {
  ot <- as.data.table(older)
  nt <- as.data.table(newer)
  ot[, `.o_row` := .I]
  nt[, `.n_row` := .I]
  ot[, `.bi` := as.Date(get(bi_col))]
  ot[, `.bo` := as.Date(get(bo_col))]
  nt[, `.bi` := as.Date(get(bi_col))]
  nt[, `.bo` := as.Date(get(bo_col))]

  is_cross <- function(bi, bo, B) !is.na(bi) & bi < B & (is.na(bo) | bo >= B)
  is_near_boundary <- function(bi, B, W) !is.na(bi) & abs(as.integer(bi - B)) <= W

  o_cross <- ot[is_cross(`.bi`, `.bo`, boundary)]
  n_cross <- nt[is_cross(`.bi`, `.bo`, boundary)]
  # Newer matching pool also includes rows whose bi is within drift_days of the
  # boundary on either side — covers the "cross-boundary duplicate" scenario
  # where the same stint's bi got recorded as bi.n >= B in newer while bi.o < B
  # in older.
  n_cand <- nt[is_cross(`.bi`, `.bo`, boundary) |
               is_near_boundary(`.bi`, boundary, drift_days)]

  cat(sprintf(
    "[%s] older crossers: %d  newer crossers: %d  newer match-pool: %d\n",
    label, nrow(o_cross), nrow(n_cross), nrow(n_cand)
  ))

  assemble_out <- function(matched_o_rows = integer(),
                           matched_n_rows = integer(),
                           merged = NULL) {
    o_keep <- ot[`.bi` < boundary & !(`.o_row` %in% matched_o_rows)]
    o_keep[, c(".o_row", ".bi", ".bo") := NULL]

    n_post <- nt[(`.bi` >= boundary | is.na(`.bi`)) & !(`.n_row` %in% matched_n_rows)]
    n_post[, c(".n_row", ".bi", ".bo") := NULL]

    n_rescued <- n_cross[!(`.n_row` %in% matched_n_rows)]
    n_rescued_count <- nrow(n_rescued)
    if (n_rescued_count) n_rescued[, c(".n_row", ".bi", ".bo") := NULL]

    n_parts <- list(n_post, n_rescued)
    if (!is.null(merged) && nrow(merged)) {
      merged_clean <- copy(merged)
      drop_cols <- intersect(c(".n_row", ".bi", ".bo"), names(merged_clean))
      if (length(drop_cols)) merged_clean[, (drop_cols) := NULL]
      n_parts <- c(n_parts, list(merged_clean))
    }

    n_out <- rbindlist(n_parts, use.names = TRUE, fill = TRUE)

    list(
      older = as_tibble(o_keep),
      newer = as_tibble(n_out),
      n_rescued = n_rescued_count
    )
  }

  if (nrow(o_cross) == 0 || nrow(n_cand) == 0) {
    out <- assemble_out()
    cat(sprintf("[%s] 1-to-1 collapses: 0  ambiguous pairs skipped: 0  rescued: %d\n",
                label, out$n_rescued))
    return(c(out, list(n_collapsed = 0L, n_ambiguous = 0L)))
  }

  # extra_match_cols must be present in BOTH older and newer; coerce to
  # character so type mismatches across releases don't break the join.
  if (length(extra_match_cols)) {
    missing_o <- setdiff(extra_match_cols, names(ot))
    missing_n <- setdiff(extra_match_cols, names(nt))
    if (length(missing_o) || length(missing_n)) {
      stop(sprintf("extra_match_cols missing from older=[%s] newer=[%s]",
                   paste(missing_o, collapse = ","),
                   paste(missing_n, collapse = ",")))
    }
  }

  ok_extra <- if (length(extra_match_cols)) {
    o_cross[, lapply(.SD, as.character), .SDcols = extra_match_cols]
  } else NULL
  nk_extra <- if (length(extra_match_cols)) {
    n_cand[, lapply(.SD, as.character), .SDcols = extra_match_cols]
  } else NULL

  ok <- cbind(
    o_cross[, .(`.o_row`, bi = `.bi`, bo = `.bo`,
                fac = get(fac_col), gender = get(gender_col),
                citizen = get(citizen_col))],
    ok_extra
  )
  nk <- cbind(
    n_cand[, .(`.n_row`, bi = `.bi`, bo = `.bo`,
               fac = get(fac_col), gender = get(gender_col),
               citizen = get(citizen_col))],
    nk_extra
  )

  base_keys <- c("fac", "gender", "citizen", extra_match_cols)

  # Layered matching, priority order. After each stage, locked rows are removed
  # from the candidate pool so later (looser) stages cannot create competing
  # candidates for already-matched rows.
  #
  #   Stage 0: exact (bi, bo, fac, gender, citizen). Highest confidence.
  #   Stage 1: exact (bi, fac, gender, citizen) + bo drift in (0, drift_days].
  #   Stage 2: exact (bo, fac, gender, citizen) + bi drift in (0, drift_days].
  #
  # Within each stage, only 1-to-1 pairs at the stage's key are locked.
  # Treat NA==NA as a match for bo in Stage 0/1.

  filter_one_to_one <- function(pairs_dt) {
    if (nrow(pairs_dt) == 0L) return(list(locked = pairs_dt, ambiguous = 0L))
    oc <- pairs_dt[, .N, by = `.o_row`]; setnames(oc, "N", "n_o")
    nc <- pairs_dt[, .N, by = `.n_row`]; setnames(nc, "N", "n_n")
    counted <- pairs_dt[oc, on = ".o_row"][nc, on = ".n_row"]
    locked <- counted[n_o == 1L & n_n == 1L, .(`.o_row`, `.n_row`)]
    list(locked = locked, ambiguous = nrow(counted) - nrow(locked))
  }

  locked_pairs <- data.table(`.o_row` = integer(), `.n_row` = integer())
  ambiguous_total <- 0L
  stage_counts <- integer(3)

  block_drop_o_rows <- integer()
  remaining_ok <- function() ok[!(`.o_row` %in% locked_pairs$`.o_row`) &
                                !(`.o_row` %in% block_drop_o_rows)]
  remaining_nk <- function() nk[!(`.n_row` %in% locked_pairs$`.n_row`)]

  # --- Stage 0 ---
  jA0 <- merge(remaining_ok(), remaining_nk(),
               by = c("bi", base_keys),
               suffixes = c(".o", ".n"), allow.cartesian = TRUE)
  jA0 <- jA0[((is.na(bo.o) & is.na(bo.n)) |
              (!is.na(bo.o) & !is.na(bo.n) & bo.o == bo.n)),
             .(`.o_row`, `.n_row`)]
  r0 <- filter_one_to_one(jA0)
  locked_pairs <- rbindlist(list(locked_pairs, r0$locked))
  ambiguous_total <- ambiguous_total + r0$ambiguous
  stage_counts[1] <- nrow(r0$locked)

  # --- Stage 0b: optional block-merge of ambiguous groups where n_o == n_n ---
  # These are groups of >1 stints in BOTH releases sharing the exact strict key.
  # We can't pair them individually, but the same-count strongly implies they
  # represent the same set of stints (anonymization shuffles per-row IDs
  # across releases). Drop older's rows; keep newer's rows as-is. No field
  # coalesce is attempted because rows can't be paired uniquely.
  n_block_merged_pairs <- 0L
  if (block_merge_equal_n && nrow(jA0) > 0L) {
    # Re-extract counts per (.o_row, .n_row) match — same as filter_one_to_one
    # but we need to identify equal-n groups by strict key, not by row.
    # Re-derive via the join key on the remaining pool.
    rem_ok <- ok[!(`.o_row` %in% locked_pairs$`.o_row`)]
    rem_nk <- nk[!(`.n_row` %in% locked_pairs$`.n_row`)]
    grp_keys <- c("bi", "bo", base_keys)
    # NA bo treated as a distinct level — group both NAs together
    co <- rem_ok[, .N, by = grp_keys]; setnames(co, "N", "n_o")
    cn <- rem_nk[, .N, by = grp_keys]; setnames(cn, "N", "n_n")
    eqn <- merge(co, cn, by = grp_keys)[n_o == n_n & n_o > 1L]
    if (nrow(eqn) > 0L) {
      drop_o <- merge(rem_ok, eqn[, ..grp_keys], by = grp_keys)
      block_drop_o_rows <- drop_o$`.o_row`
      n_block_merged_pairs <- sum(eqn$n_o)
      cat(sprintf("[%s]   block-merge (n_o==n_n>1 at strict key): %d groups, %d rows per side dropped on older\n",
                  label, nrow(eqn), n_block_merged_pairs))
    }
  }

  # --- Stage 1 ---
  jA1 <- merge(remaining_ok(), remaining_nk(),
               by = c("bi", base_keys),
               suffixes = c(".o", ".n"), allow.cartesian = TRUE)
  jA1 <- jA1[!is.na(bo.o) & !is.na(bo.n) &
             abs(as.integer(bo.n - bo.o)) > 0L &
             abs(as.integer(bo.n - bo.o)) <= drift_days,
             .(`.o_row`, `.n_row`)]
  r1 <- filter_one_to_one(jA1)
  locked_pairs <- rbindlist(list(locked_pairs, r1$locked))
  ambiguous_total <- ambiguous_total + r1$ambiguous
  stage_counts[2] <- nrow(r1$locked)

  # --- Stage 2 ---
  jB2 <- merge(remaining_ok()[!is.na(bo)], remaining_nk()[!is.na(bo)],
               by = c("bo", base_keys),
               suffixes = c(".o", ".n"), allow.cartesian = TRUE)
  jB2 <- jB2[!is.na(bi.o) & !is.na(bi.n) &
             abs(as.integer(bi.n - bi.o)) > 0L &
             abs(as.integer(bi.n - bi.o)) <= drift_days,
             .(`.o_row`, `.n_row`)]
  r2 <- filter_one_to_one(jB2)
  locked_pairs <- rbindlist(list(locked_pairs, r2$locked))
  ambiguous_total <- ambiguous_total + r2$ambiguous
  stage_counts[3] <- nrow(r2$locked)

  cat(sprintf("[%s]   stage breakdown — exact: %d, bo-drift: %d, bi-drift: %d\n",
              label, stage_counts[1], stage_counts[2], stage_counts[3]))

  one_to_one <- locked_pairs
  n_collapsed <- nrow(one_to_one)
  n_ambiguous <- ambiguous_total

  if (n_collapsed == 0L) {
    out <- assemble_out(matched_o_rows = block_drop_o_rows)
    cat(sprintf("[%s] 1-to-1 collapses: 0  ambiguous pairs skipped: %d  block-dropped (older): %d  rescued: %d\n",
                label, n_ambiguous, length(block_drop_o_rows), out$n_rescued))
    return(c(out, list(n_collapsed = 0L, n_ambiguous = n_ambiguous,
                       n_block_dropped = length(block_drop_o_rows))))
  }

  matched_o <- ot[one_to_one$`.o_row`]
  matched_n <- nt[one_to_one$`.n_row`]

  shared_cols <- intersect(names(matched_o), names(matched_n))
  shared_cols <- setdiff(shared_cols, c(".o_row", ".n_row", ".bi", ".bo"))

  coerce_to_target <- function(target, x) {
    if (identical(class(target), class(x))) return(x)
    if (inherits(target, "Date"))    return(suppressWarnings(as.Date(x)))
    if (inherits(target, "POSIXct")) return(suppressWarnings(as.POSIXct(x)))
    if (is.integer(target))          return(suppressWarnings(as.integer(x)))
    if (is.numeric(target))          return(suppressWarnings(as.numeric(x)))
    if (is.character(target))        return(as.character(x))
    if (is.logical(target))          return(suppressWarnings(as.logical(x)))
    x
  }

  for (col in shared_cols) {
    new_val <- matched_n[[col]]
    old_val <- matched_o[[col]]
    if (!identical(class(new_val), class(old_val))) {
      coerced <- tryCatch(coerce_to_target(new_val, old_val),
                          error = function(e) NULL)
      if (is.null(coerced)) {
        new_val <- as.character(new_val)
        old_val <- as.character(old_val)
        matched_n[, (col) := new_val]
      } else {
        old_val <- coerced
      }
    }
    matched_n[, (col) := fifelse(is.na(new_val), old_val, new_val)]
  }

  older_only_cols <- setdiff(names(matched_o), names(matched_n))
  for (col in older_only_cols) {
    matched_n[, (col) := matched_o[[col]]]
  }

  # block_drop_o_rows: older rows we drop without pairing (block-merge mode).
  # Their newer-side counterparts are kept as-is in n_post / n_rescued.
  out <- assemble_out(
    matched_o_rows = c(one_to_one$`.o_row`, block_drop_o_rows),
    matched_n_rows = one_to_one$`.n_row`,
    merged = matched_n
  )

  cat(sprintf("[%s] 1-to-1 collapses: %d  ambiguous pairs skipped: %d  block-dropped (older): %d  rescued: %d\n",
              label, n_collapsed, n_ambiguous, length(block_drop_o_rows), out$n_rescued))

  c(out, list(n_collapsed = n_collapsed, n_ambiguous = n_ambiguous,
              n_block_dropped = length(block_drop_o_rows)))
}
