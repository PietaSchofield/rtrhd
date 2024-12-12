#' read excel sheets to list
#'
#' @export
read_excel_sheets_to_list <- function(file_path) {
  # Get all sheet names from the Excel file
  sheet_names <- readxl::excel_sheets(file_path)

  # Read each sheet and store in a named list
  sheet_list <- lapply(sheet_names, function(sheet) {
    readxl::read_excel(file_path, sheet = sheet)
  })

  # Name the list elements with their corresponding sheet names
  names(sheet_list) <- sheet_names

  return(sheet_list)
}
