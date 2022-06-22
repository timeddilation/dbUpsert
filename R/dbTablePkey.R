#' Gets Primary Key from a SQL Table
#'
#' @param conn A DBI Connection Object
#' @param name A table name in the DB

.dbTablePkey <- function(conn, name) {
  rdbms <- conn |> class() |> as.character()

  if (rdbms == "PqConnection") {
    pkey <- .dbTablePkey_postgres(conn, name)
  # } else if (rdbms == "MySQLConnection") {
  #   stop("Implementation for MySQLConnection is a stub.")
  #   # pkey <- .dbTablePkey_mysql(conn, name)
  # } else if (rdbms == "Microsoft SQL Server") {
  #   stop("Implementation for Microsoft SQL Server is a stub.")
  #   # pkey <- .dbTablePkey_mssql(conn, name)
  } else {
    stop("Cannot query for primary key in this RDBMS. You will need to manually specify `value_pkey` or `join_on`")
  }

  if (length(pkey) < 1) {
    stop(paste0("Target table `", name, "` has no primary key. Cannot do update."))
  }

  return(pkey)
}
