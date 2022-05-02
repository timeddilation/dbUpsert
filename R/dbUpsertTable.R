#' Upsert data.frame to SQL DB

dbUpsertTable <- function(
  conn,
  name,
  value,
  stage_table = NULL,
  overwrite_stage_table = TRUE,
  verbose = F
) {
  ##############################################################################
  # check if table exists
  ##############################################################################
  if (dbExistsTable(conn, name) == FALSE) {
    stop(paste0("Target table `", name, "` does not exist."))
  }

  ##############################################################################
  # Get primary key column(s) from table
  ##############################################################################
  if (verbose == TRUE) {
    cat("Querying table primary key columns\n")
  }
  value_pkey <- dbTablePkey(conn, name)

  if (verbose == TRUE) {
    cat(paste0(
      "  ",
      paste0(value_pkey, collapse = ",\n  "),
      "\n"
    ))
  }

  ### Check if all PKs are provided in value
  missing_pkey_cols <- value_pkey[! value_pkey %in% names(value)]

  if (length(missing_pkey_cols) > 0) {
    stop(paste0(
      "Table primary key column(s) `",
      paste0(missing_pkey_cols, collapse = ", "),
      "` was not provided."
    ))
  }
  rm(missing_pkey_cols)

  ### check if any pkeys columns are duplicated in provided data
  value_pkey_provided <- names(value)[names(value) %in% value_pkey]
  duplicated_pkey <- value_pkey_provided[duplicated(value_pkey_provided)]

  if (length(duplicated_pkey) > 0) {
    stop(paste0(
      "Primary key column(s) provided more than once: ",
      paste0(duplicated_pkey, collapse = ", ")
    ))
  }
  rm(value_pkey_provided, duplicated_pkey)

  ### check for any duplicate keys, cannot do upserts
  provided_rows <- value[, value_pkey, drop = FALSE] |> nrow()
  provided_unique_rows <- value[, value_pkey, drop = FALSE] |> unique() |> nrow()

  if (provided_rows > provided_unique_rows) {
    stop("More than one row with the same primary key cannot be upserted.")
  }
  rm(provided_rows, provided_unique_rows)
  ##############################################################################
  # TODO: Get non-pk columns and nullability
  ##############################################################################

  ##############################################################################
  # TODO: Check all not-nulls are provided
  ##############################################################################

  ##############################################################################
  # TODO: Remove columns not in target table
  ##############################################################################

  ##############################################################################
  # Check for duplicated column names
  ##############################################################################
  duplicate_cols <- value |>
    names() |>
    duplicated() |>
    {\(x) names(value)[x]}()

  if (length(duplicate_cols) > 0) {
    stop(paste0(
      "Cannot upsert duplicated column names: ",
      paste0(duplicate_cols, collapse = ", ")
    ))
  }
  ##############################################################################
  # TODO: Create vectors for insert and update cols
  ##############################################################################


  ##############################################################################
  # set staging table name
  ##############################################################################
  if (is.null(stage_table) == TRUE) {
    stage_table <- paste0("stage_", name)
  }
  if (verbose == TRUE) {
    cat(paste0("Writing data to staging table: ", stage_table, "\n"))
  }

  return(TRUE)
}
