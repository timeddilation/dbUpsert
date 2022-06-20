dbUpdateStatement_postgres <- function(
  conn,
  target_table,
  staging_table,
  join_cols = NA,
  update_cols = NA
) {
  # check if join columns are valid
  join_cols <- as.character(join_cols)
  if (length(join_cols) == 0) {
    stop("Must provide at least one column to join on.")
  }
  # check if update columns are valid
  if (length(update_cols) == 0) {
    stop("Must provide at least one column to update.")
  }

  target_table <- dbQuoteIdentifier(
    conn = conn,
    x = target_table
  ) |> as.character()

  staging_table <- dbQuoteIdentifier(
    conn = conn,
    x = staging_table
  ) |> as.character()

  join_cols <- dbQuoteIdentifier(
    conn = conn,
    x = join_cols
  ) |> as.character()

  update_cols <- dbQuoteIdentifier(
    conn = conn,
    x = update_cols
  ) |>
    as.character() |>
    {\(x) paste0(x, " = st.", x)}() |>
    paste0(collapse = "\n  ,")

  update_statement <- paste0(
    "UPDATE ", target_table, " tt SET\n",
    "  ", update_cols, "\n",
    "FROM ", staging_table, " st\n",
    "WHERE tt.", join_cols[1],  " = st.", join_cols[1], "\n",
    ifelse(
      length(join_cols) > 1,
      paste0("AND tt.", join_cols[-1], " = st.", join_cols[-1]) |> paste0(collapse = "\n"),
      ""
    ),
    ";"
  )

  return(update_statement)
}
