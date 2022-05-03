#' Generate Upsert SQL Statement
#'
#' @param conn A DBI Connection Object
#' @param target_table A table name in the DB to upsert to
#' @param staging_table A table name in the DB containing data to upsert
#' @param table_pkey A character vector of column names used as the primary key
#' @param insert_cols A character vector of column names to insert, should include the PKs
#' @param update_cols A character vector of column names to update, should exclude the PKs

dbUpsertStatement <- function(
  conn,
  target_table,
  staging_table,
  table_pkey,
  insert_cols,
  update_cols
) {
  require(glue)
  rdbms <- conn |> class() |> as.character()

  if (rdbms == "PqConnection") {
    return(dbUpsertStatement_postgres(
      conn,
      target_table,
      staging_table,
      table_pkey,
      insert_cols,
      update_cols
    ))
  } else {
    stop("No implementation for SQL backend.")
  }
}
