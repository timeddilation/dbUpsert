dbUpdateTable <- function(
  conn,
  name,
  value,
  join_on = NULL,
  stage_table = NULL,
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
  # Try to guess `join_on` if not provided explicitly
  ##############################################################################
  table_cols <- dbColumnInfoExtended(conn, name)

  # first, check if there is an identity column, and use the identity column
  if (is.null(join_on)) {
    if (verbose == TRUE) {
      cat("Checking for identity column\n")
    }

    identity_col <- table_cols[table_cols$is_identity == TRUE, "column_name"]

    if (length(identity_col) > 0) {
      join_on <- identity_col

      if (verbose == TRUE) {
        cat(paste0(
          "  ",
          paste0(identity_col, collapse = ",\n  "),
          "\n"
        ))
      }
    } else {
      if (verbose == TRUE) {
        cat("No identity column found\n")
      }
    }
    rm(identity_col)
  }

  # if there are no identity columns, then check for primary keys
  if (is.null(join_on)) {
    if (verbose == TRUE) {
      cat("Querying table primary key columns\n")
    }
    value_pkey <- .dbTablePkey(conn, name)

    if (verbose == TRUE) {
      cat(paste0(
        "  ",
        paste0(value_pkey, collapse = ",\n  "),
        "\n"
      ))
    }

    join_on <- value_pkey
    rm(value_pkey)
  }

  ##############################################################################
  # Check if all values for join_on are in the target SQL table
  ##############################################################################
  join_on_missing <- join_on[!join_on %in% table_cols$column_name]

  if (length(join_on_missing) > 0) {
    stop(paste0(
      "The following columns were specified to join on, but are not in the target SQL table: ",
      paste0(join_on_missing, collapse = ", ")
    ))
  }
  rm(join_on_missing)

  ##############################################################################
  # Check if all values for join_on are in the provided value table
  ##############################################################################
  join_on_missing <- join_on[!join_on %in% names(value)]

  if (length(join_on_missing) > 0) {
    stop(paste0(
      "The following columns were specified to join on, but are not in the provided `value` table: ",
      paste0(join_on_missing, collapse = ", ")
    ))
  }
  rm(join_on_missing)

  ##############################################################################
  # Check if any of the join_on values are duplicated
  ##############################################################################
  provided_rows <- value[, join_on, drop = FALSE] |> nrow()
  provided_unique_rows <- value[, join_on, drop = FALSE] |> unique() |> nrow()

  if (provided_rows > provided_unique_rows) {
    stop(paste0(
      "More than one row with the same combination of `join_on` provided. ",
      "All rows must have unique join_on column(s) otherwise SQL will reject the update statement."
    ))
  }
  rm(provided_rows, provided_unique_rows)

  ##############################################################################
  # Remove columns not in target table
  ##############################################################################
  all_target_cols <- c(join_on, table_cols$column_name)
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
      "Cannot update duplicated column names: ",
      paste0(duplicate_cols, collapse = ", ")
    ))
  }

  ##############################################################################
  # Create vector for update cols
  ##############################################################################
  update_cols <- names(value)[! names(value) %in% join_on]

  if (verbose == TRUE) {
    cat(paste0(
      "Columns to join by: ", paste0(join_on, collapse = ", "), "\n",
      "Columns being updated: ", paste0(update_cols, collapse = ", "), "\n"
    ))
  }

  ##############################################################################
  # set staging table name
  ##############################################################################
  if (is.null(stage_table) == TRUE) {
    stage_table <- paste0("stage_", name)
  }
  if (verbose == TRUE) {
    cat(paste0("Writing data to staging table: ", stage_table, "\n"))
  }

  ##############################################################################
  # Stage data in database
  ##############################################################################
  dbWriteTable(
    conn = conn,
    name = stage_table,
    value = value,
    overwrite = overwrite_stage_table
  )

  ##############################################################################
  # Send update statement
  ##############################################################################
  update_statement <- .dbUpdateStatement(
    conn = conn,
    target_table = name,
    staging_table = stage_table,
    join_cols = join_on,
    update_cols = update_cols
  )

  if (verbose == TRUE) {
    cat(paste0(
      "Generated SQL Upsert Command:\n<SQL>\n",
      update_statement,
      "\n</SQL>\n"
    ))
  }

  update_res <- dbSendStatement(
    conn = conn,
    statement = update_statement
  )
  dbClearResult(update_res)

  ##############################################################################
  # Remove staging table
  ##############################################################################
  dbRemoveTable(conn, stage_table)

  return(TRUE)
}
