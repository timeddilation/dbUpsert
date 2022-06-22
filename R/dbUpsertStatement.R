#' Generate Upsert SQL Statement
#'
#' This function should never be called directly.
#' It is an internal function to the package.
#' Generates ANSI compliant SQL statements to update existing records, and insert new records.
#' This function returns a list of 2 SQL statements.
#' The first element of text in the list is the SQL UPDATE statement.
#' The second element of text in the list is the SQL INSERT statement.
#' It is intended that both statements are sent in the same transaction.
#'
#' @param conn A DBI Connection Object
#' @param target_table A table name in the DB to upsert to
#' @param staging_table A table name in the DB containing data to upsert
#' @param table_pkey A character vector of column names used as the primary key
#' @param insert_cols A character vector of column names to insert, should include the PKs
#' @param update_cols A character vector of column names to update, should exclude the PKs

.dbUpsertStatement <- function(
  conn,
  target_table,
  staging_table,
  table_pkey,
  insert_cols,
  update_cols
) {
  upsert_update_statement <- .dbUpdateStatement(
    conn = conn,
    target_table = target_table,
    staging_table = staging_table,
    join_cols = table_pkey,
    update_cols = update_cols
  )

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
    as.character()

  upsert_insert_statement <- paste0(
    "INSERT INTO ", target_table, "(\n",
    "  ", insert_cols, "\n",
    ")\n",
    "SELECT\n",
    "  ", insert_cols, "\n",
    "FROM ", staging_table, "\n",
    "WHERE NOT EXISTS (\n",
    "  SELECT 1 FROM ", target_table, "\n",
    "  WHERE ", target_table, ".", table_pkey[1],  " = ", staging_table, ".", table_pkey[1], "\n",
    ifelse(
      length(table_pkey) > 1,
      paste0("  AND ", target_table, ".", table_pkey[-1], " = ", staging_table, ".", table_pkey[-1]) |>
        paste0(collapse = "\n"),
      ""
    ),
    ");"
  )

  return(list(upsert_update_statement, upsert_insert_statement))
}
