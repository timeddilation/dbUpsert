#' Gets SQL DB Version
#'
#' Given a DBI connection, evaluate the information to extract the DB Version.
#' This may be necessary when executing RDBMS-specific SQL syntax.
#'
#' @param conn A DBI Connection Object
#'
#' @examples
#' db_con <- dbConnect(...)
#' dbVersion(db_con)

dbVersion <- function(conn) {
  rdbms <- conn |> class() |> as.character()

  if (rdbms == "PqConnection") {
    return(dbGetInfo(conn)[["db.version"]])
  } else if (rdbms == "MySQLConnection") {
    return(dbGetInfo(conn)[["serverVersion"]])
  } else if (rdbms == "Microsoft SQL Server") {
    return(dbGetInfo(conn)[["db.version"]])
  } else {
    return("unknown")
  }

  return(sql_version)
}
