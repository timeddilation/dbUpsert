#' Gets Primary Key from Postgres Table
#'
#' @param conn A DBI Connection Object
#' @param name A table name in the DB

.dbTablePkey_postgres <- function(conn, name) {
  dbGetQuery(
    conn = conn,
    statement = "
      SELECT c.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage AS ccu
          USING (constraint_schema, constraint_name)
      JOIN information_schema.columns AS c
          ON c.table_schema = tc.constraint_schema
          AND tc.table_name = c.table_name
          AND ccu.column_name = c.column_name
      WHERE constraint_type = 'PRIMARY KEY'
      AND tc.table_name = $1",
    params = name
  )$column_name
}
