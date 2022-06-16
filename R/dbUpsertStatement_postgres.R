#' Generate Upsert SQL Statement for Postgres
#'
#' Should not be called directly. Use `dbUpsertStatement()` instead.
#'
#' @param conn A DBI Connection Object
#' @param target_table A table name in the DB to upsert to
#' @param staging_table A table name in the DB containing data to upsert
#' @param table_pkey A character vector of column names used as the primary key
#' @param insert_cols A character vector of column names to insert, should include the PKs
#' @param update_cols A character vector of column names to update, should exclude the PKs

dbUpsertStatement_postgres <- function(
  conn,
  target_table,
  staging_table,
  table_pkey,
  insert_cols,
  update_cols
) {
  target_table <- dbQuoteIdentifier(
    conn = conn,
    x = target_table
  ) |> as.character()

  insert_cols <- dbQuoteIdentifier(
    conn = conn,
    x = insert_cols
  ) |>
    as.character() |>
    paste0(collapse = "\n  ,")

  staging_table <- dbQuoteIdentifier(
    conn = conn,
    x = staging_table
  ) |> as.character()

  table_pkey <- dbQuoteIdentifier(
    conn = conn,
    x = table_pkey
  ) |>
    as.character() |>
    paste0(collapse = ", ")

  update_cols <- dbQuoteIdentifier(
    conn = conn,
    x = update_cols
  ) |>
    as.character() |>
    {\(x) paste0(x, " = excluded.", x)}() |>
    paste0(collapse = "\n  ,")

  upsert_statement <- paste0(
    "INSERT INTO ", target_table, "(\n",
    "  ", insert_cols, "\n",
    ")\n",
    "SELECT\n",
    "  ", insert_cols, "\n",
    "FROM ", staging_table, "\n",
    "ON CONFLICT (", table_pkey, ") DO UPDATE SET\n",
    "  ", update_cols
  )

  return(upsert_statement)
}
