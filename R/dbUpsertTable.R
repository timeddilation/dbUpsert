#' Upsert (insert and update) data.frame to SQL table
#'
#' Only works with primary keys, not identities/sequences.
#' The target SQL table must have a primary key, tables without primary keys cannot perform conflict detection.
#' Primary keys cannot be updated with this function, even if the SQL table allows keys to be changed.
#' If you are trying to upsert a table that does not have primary key, instead consider first using `dbAppendTable()` to write new data, then `dbUpdateTable()` to update existing values.
#' Use the `verbose = TRUE` argument to show details of the function's progress in the console.
#' This function returns `TRUE` if it succeeds.
#'
#' @param conn A DBI Connection Object
#' @param name A table name in the DB to upsert to
#' @param value A dataframe object containing data to upsert
#' @param value_pkey A character vector of column names that form the SQL table primary key. For supported SQL backends, this is queried for you. Otherwise, you will have to manually specify.
#' @param staging_table A string of the table name to create to stage data. If not provided, a new table name will be generated.
#' @param overwrite_stage_table A boolean indicating if you want to drop the staging table (if it already exists). If it does already exist, and this value is `false`, then the upsert will fail.
#' @param verbose A boolean indicating whether or not to print steps executed in the console

dbUpsertTable <- function(
  conn,
  name,
  value,
  value_pkey = NA,
  stage_table = paste0("stage_", name),
  overwrite_stage_table = TRUE,
  verbose = FALSE
) {
  # TODO: Check if table has non-updatable columns, remove these from upsert statement

  ##############################################################################
  # check if table exists
  ##############################################################################
  if (dbExistsTable(conn, name) == FALSE) {
    stop(paste0("Target table `", name, "` does not exist."))
  }

  ##############################################################################
  # Get primary key column(s) from table
  ##############################################################################
  if (is.na(value_pkey)) {
    if (verbose == TRUE) {
      cat("Querying table primary key columns\n")
    }
    value_pkey <- .dbTablePkey(conn, name)
  }

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

  ##############################################################################
  # Check if any pkeys columns are duplicated in provided data
  ##############################################################################
  value_pkey_provided <- names(value)[names(value) %in% value_pkey]
  duplicated_pkey <- value_pkey_provided[duplicated(value_pkey_provided)]

  if (length(duplicated_pkey) > 0) {
    stop(paste0(
      "Primary key column(s) provided more than once: ",
      paste0(duplicated_pkey, collapse = ", ")
    ))
  }
  rm(value_pkey_provided, duplicated_pkey)

  ##############################################################################
  # Check for any duplicate keys, cannot do upserts
  ##############################################################################
  provided_rows <- value[, value_pkey, drop = FALSE] |> nrow()
  provided_unique_rows <- value[, value_pkey, drop = FALSE] |> unique() |> nrow()

  if (provided_rows > provided_unique_rows) {
    stop("More than one row with the same primary key cannot be upserted.")
  }
  rm(provided_rows, provided_unique_rows)

  ##############################################################################
  # Get extended columns data
  ##############################################################################
  if (verbose == TRUE) {
    cat("Querying table column info\n")
  }
  table_cols <- dbColumnInfoExtended(conn, name)
  table_cols <- table_cols[!table_cols$column_name %in% value_pkey, ]

  if (verbose == TRUE) {
    cat(paste0(
      "  ",
      paste0(
        table_cols$column_name,
        " (", table_cols$data_type, ") ",
        ifelse(table_cols$is_nullable == TRUE, "NULL", "NOT NULL")
      ) |> paste0(collapse = "\n  "),
      "\n"
    ))
  }

  ##############################################################################
  # check if any identity generation is always
  ##############################################################################
  always_generated_ids <- table_cols[table_cols$can_insert_id == FALSE, "column_name"]

  if (length(always_generated_ids) > 0) {
    stop(paste0(
      "Cannot upsert to table with always generated identities. ",
      "Consider two separate commands to `dbAppendTable` then `dbUpdateTable` instead."
    ))
  }
  rm(always_generated_ids)
  ##############################################################################
  # Check all not-nulls are provided
  ##############################################################################
  target_not_nulls <- table_cols[table_cols$is_nullable == FALSE, "column_name"]
  missing_not_nulls <- target_not_nulls[!target_not_nulls %in% names(value)]

  if (length(missing_not_nulls) > 0) {
    stop(paste0(
      "Not nullable columns are missing, cannot insert with missing values: ",
      paste0(missing_not_nulls, collapse = ", ")
    ))
  }
  rm(target_not_nulls, missing_not_nulls)

  ##############################################################################
  # TODO: Check all not nulls all have values, no NA's
  ##############################################################################

  ##############################################################################
  # Remove columns not in target table
  ##############################################################################
  all_target_cols <- c(value_pkey, table_cols$column_name)
  cols_not_exist <- names(value)[!names(value) %in% all_target_cols]

  if (length(cols_not_exist) > 0) {
    value[cols_not_exist] <- NULL

    warning(paste0(
      "The following provided columns do not exist in the DB table: ",
      paste0(cols_not_exist, collapse = ", "),
      ". These columns will be removed before upserting to DB."
    ))
  }
  rm(cols_not_exist)

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
  # Create vectors for insert and update cols
  ##############################################################################
  insert_cols <- names(value)
  update_cols <- names(value)[! names(value) %in% value_pkey]

  if (verbose == TRUE) {
    cat(paste0(
      "Columns being inserted: ", paste0(insert_cols, collapse = ", "), "\n",
      "Columns being updated: ", paste0(update_cols, collapse = ", "), "\n"
    ))
  }

  ##############################################################################
  # Write data to staging table
  ##############################################################################
  if (verbose == TRUE) {
    cat(paste0("Writing data to staging table: ", stage_table, "\n"))
  }

  dbWriteTable(
    conn = conn,
    name = stage_table,
    value = value,
    overwrite = overwrite_stage_table
  )

  ##############################################################################
  # create upsert statement, cat if verbose, send statement
  ##############################################################################
  upsert_statements <- .dbUpsertStatement(
    conn = conn,
    target_table = name,
    staging_table = stage_table,
    table_pkey = value_pkey,
    insert_cols = insert_cols,
    update_cols = update_cols
  )

  if (verbose == TRUE) {
    cat(paste0(
      "Generated SQL Upsert Command:\n<SQL>\n",
      upsert_statements[[1]], "\n",
      upsert_statements[[2]],
      "\n</SQL>\n"
    ))
  }

  dbWithTransaction(
    conn = conn,
    {
      update_res <- dbSendStatement(conn, upsert_statements[[1]])
      dbClearResult(update_res)

      insert_res <- dbSendStatement(conn, upsert_statements[[2]])
      dbClearResult(insert_res)
    }
  )

  ##############################################################################
  # Drop staging table
  ##############################################################################
  if (verbose == TRUE) {
    cat(paste0("Dropping staging table: ", stage_table, "\n"))
  }
  dbRemoveTable(conn, stage_table)

  return(TRUE)
}
