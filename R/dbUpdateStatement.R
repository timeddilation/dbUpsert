#' Generate update SQL Statement
#'
#' @param conn A DBI Connection Object
#' @param target_table A table name in the DB to upsert to
#' @param staging_table A table name in the DB containing data to upsert
#' @param join_cols A character vector of column names used as the primary key
#' @param update_cols A character vector of column names to update, should exclude the PKs

dbUpdateStatement <- function(
  conn,
  target_table,
  staging_table,
  join_cols,
  update_cols
) {
  rdbms <- conn |> class() |> as.character()

  if (rdbms == "PqConnection") {
    return(dbUpdateStatement_postgres(
      conn,
      target_table,
      staging_table,
      join_cols,
      update_cols
    ))
  } else {
    stop("No implementation for SQL backend.")
  }
}
