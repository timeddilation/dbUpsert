#' Generates update SQL Statement
#'
#' This function should never be called directly.
#' It is an internal function to the package.
#' Generates ANSI compliant SQL statement to update existing records from another table.
#'
#' @param conn A DBI Connection Object
#' @param target_table A table name in the DB to upsert to
#' @param staging_table A table name in the DB containing data to upsert
#' @param join_cols A character vector of column names used as the primary key
#' @param update_cols A character vector of column names to update, should exclude the PKs

.dbUpdateStatement <- function(
  conn,
  target_table,
  staging_table,
  join_cols = NA,
  update_cols = NA
) {
  # check if join cols are valid
  join_cols <- as.character(join_cols)
  if (length(join_cols) == 0) {
    stop("Must provide at least one column to join on.")
  }
  # check if update columns are valid
  update_cols <- as.character(update_cols)
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
    {\(x) paste0(x, " = ", staging_table, ".", x)}() |>
    paste0(collapse = "\n  ,")

  update_statement <- paste0(
    "UPDATE ", target_table, " SET\n",
    "  ", update_cols, "\n",
    "FROM ", staging_table, "\n",
    "WHERE ", target_table, ".", join_cols[1],  " = ", staging_table, ".", join_cols[1], "\n",
    ifelse(
      length(join_cols) > 1,
      paste0("AND ", target_table, ".", join_cols[-1], " = ", staging_table, ".", join_cols[-1]) |>
        paste0(collapse = "\n"),
      ""
    ),
    ";"
  )

  return(update_statement)
}
