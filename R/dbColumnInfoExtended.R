#' Extended details about table columns
#'
#' @param conn A DBI Connection Object
#' @param name A table name in the DB

dbColumnInfoExtended <- function(conn, name) {
  rdbms <- conn |> class() |> as.character()

  if (rdbms == "PqConnection") {
    col_info <- dbGetQuery(
      conn = conn,
      statement = "
        SELECT
          column_name,
          data_type,
          CASE is_nullable WHEN 'NO' THEN FALSE ELSE TRUE END AS is_nullable,
          CASE is_identity WHEN 'NO' THEN FALSE ELSE TRUE END AS is_identity,
          CASE is_generated WHEN 'NO' THEN FALSE ELSE TRUE END AS is_generated,
          CASE is_updatable WHEN 'NO' THEN FALSE ELSE TRUE END AS is_updatable,
          CASE identity_generation WHEN 'ALWAYS' THEN FALSE ELSE TRUE END AS can_insert_id
        FROM information_schema.columns
        WHERE table_name = $1;",
      params = list(name)
    )
  } else if (rdbms == "MySQLConnection") {
    stop("Implementation for MySQLConnection is a stub.")
    # col_info <- dbColumnInfoExtended_mysql(conn, name)
  } else if (rdbms == "Microsoft SQL Server") {
    stop("Implementation for Microsoft SQL Server is a stub.")
    # col_info <- dbColumnInfoExtended_mssql(conn, name)
  } else {
    stop("No implementation planned for DB backend.")
  }

  return(col_info)
}
