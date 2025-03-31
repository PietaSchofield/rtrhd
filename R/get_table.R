#' Run a SQL query on a DuckDB database
#'
#' This function connects to a DuckDB database, runs a SQL query (either provided 
#' directly or constructed from a table name and field list), and returns the result 
#' as a tibble. Optionally, the query can be run with `EXPLAIN ANALYZE` to inspect 
#' query performance.
#'
#' @param dbf Character. Path to the DuckDB database file.
#' @param sqlstr Character. A full SQL query string. If provided, this takes priority 
#'   over `tabname`, `fields`, and `whereclause`.
#' @param tabname Character. Table name to construct a query from (if `sqlstr` is NULL).
#' @param fields Character vector. Fields to select when constructing a query (default is '*').
#' @param whereclause Character. Optional WHERE clause to apply when constructing a query.
#' @param explain Logical. If TRUE, wraps the query with `EXPLAIN ANALYZE` and returns
#'   the query plan instead of the data.
#'
#' @return A tibble containing the result of the query, or a query plan if `explain = TRUE`.
#' 
#' @examples
#' get_table("mydb.duckdb", sqlstr = "SELECT * FROM patients LIMIT 10")
#' get_table("mydb.duckdb", sqlstr = "SELECT * FROM patients WHERE age > 80", explain = TRUE)
#'
#' @export
get_table <- function(dbf, sqlstr = NULL, tabname = NULL, fields = '*',
                      whereclause = NULL, explain = FALSE) {
  if (is.null(sqlstr)) {
    strsql <- str_c("
      SELECT DISTINCT
      ", paste0(fields, collapse = ", "), "
      FROM 
      ", tabname)

    if (!is.null(whereclause)) {
      strsql <- str_c(strsql, "\nWHERE ", whereclause)
    }
  } else {
    strsql <- sqlstr
  }

  if (explain) {
    strsql <- paste("PRAGMA enable_profiling='graphviz';EXPLAIN ANALYZE", strsql)
  }

  dbi <- duckdb::dbConnect(duckdb::duckdb(), dbf)
  result <- DBI::dbGetQuery(dbi, strsql) %>% tibble()
  duckdb::dbDisconnect(dbi, shutdown = TRUE)

  return(result)
}

