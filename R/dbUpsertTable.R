#' Upsert data.frame to SQL DB

dbUpsertTable <- function(
  conn,
  name,
  value,
  stage_table = NULL,
  overwrite_stage_table = TRUE
) {
  debug_internal <- list()
  # check if table exists
  if (dbExistsTable(conn, name) == FALSE) {
    stop(paste0("Target table `", name, "` does not exist."))
  }

  # get primary key if not provided
  # this function handles error if no pkey exists
  value_pkey <- dbTablePkey(conn, name)

  # check if pkey is provided in value
  if (length(names(value)[names(value) %in% value_pkey]) != length(value_pkey)) {
    stop(paste0(
      "Table primary key `",
      paste0(value_pkey, collapse = ", "),
      "` was not provided."
    ))
  }
  debug_internal <- c(debug_internal, list("pkey" = value_pkey))

  # set staging table name
  if (is.null(stage_table) == TRUE) {
    stage_table <- paste0("stage_", name)
  }
  debug_internal <- c(debug_internal, list("stage_table" = stage_table))

  return(debug_internal)
}
