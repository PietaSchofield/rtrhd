#' Plot a patients time line from timeline data
#'
#' @import ggplot2 
#'
#' @export
plot_group <- function(data, group, x_min, x_max,shapevals=c("Primary"=21,"Secondary"=24),dbug=F) {
  
  sub_df <- data %>% dplyr::filter(TrackName == group)

  if(dbug){
    print(paste("Processing group:", group))
    print(nrow(sub_df))  # Should NOT be 0
    print(str(sub_df))    # Check the structure of the filtered dataset
  }

  if(nrow(sub_df) == 0) {
    return(ggplot() + theme_void() + ggtitle(paste("No Data for", group)))
  }

  p <- ggplot2::ggplot()
  pdat <- sub_df %>% dplyr::filter(Type == "Period")
  # Period Events (Bars)
  if(nrow(pdat) > 0) {
    if(nrow(pdat %>% dplyr::filter(SubType == "Variable"))) {
      p <- p + ggplot2::geom_rect(
        data = pdat %>% dplyr::filter(SubType == "Variable"),
        aes(xmin = StartDate, xmax = EndDate, ymin = 0, ymax = Frequency),
        fill = "steelblue", color = "black", alpha = 0.7
      )
      p <- p + theme(axis.title.y = element_text(size = 10), axis.text.y = element_text(size = 8)) +
            labs(y = group) + scale_y_continuous() 

    }
    if(nrow(pdat %>% dplyr::filter(SubType == "Fixed"))) {
      p <- p + geom_segment(
        data = pdat %>% dplyr::filter(SubType == "Fixed"),
        aes(x = StartDate, xend = EndDate, y = 1, yend = 1),
        linewidth = 10, color = "gray"
      )  
      p <- p + geom_text(
        data = pdat %>% distinct(TrackName, StartDate, EndDate, .keep_all = TRUE),
        aes(x = StartDate + (EndDate - StartDate) / 2, y = 1, label = group),
        color = "black", size = 3, fontface = "bold"
      ) + labs(y = " ")
    }
  }

  # One-Off Events (Points) - Fixing Fill Issue
  edat <- sub_df %>% dplyr::filter(Type == "Event")
  if(nrow(edat) > 0) {
    if(nrow(edat %>% dplyr::filter(SubType == "Fixed"))) {
       ncolg <- edat %>% dplyr::filter(SubType == "Fixed") %>% select(Condition) %>% unique() %>% nrow()
       p <- p + geom_point(
        data = edat %>% dplyr::filter(SubType == "Fixed"),
        aes(x = StartDate, y = EventY, fill = Condition, shape = Sector),
        size = 4, color = "black", stroke = 1  # Added stroke
      ) + scale_y_continuous(limits = c(0, 1)) 
      p <- p + scale_fill_brewer(palette = "Set1", na.value = "gray", name=group) +
               scale_shape_manual(values = shapevals) +
               guides(fill=guide_legend(override.aes=list(shape=21),ncol=(ncolg%/%4+1))) +
               labs(y= group)
    }
    if(nrow(edat %>% dplyr::filter(SubType == "Variable"))) {
       p <- p + geom_point(
        data = edat %>% dplyr::filter(SubType == "Variable"),
        aes(x = StartDate, y = EventY, fill = Condition, shape = Sector),
        size = 4, color = "black", stroke = 1  # Added stroke
      ) + scale_y_continuous() 
      p <- p + labs(y= group)
    }
  }

  vdat <- sub_df %>% dplyr::filter(Type == "V-Line")
  # Add Vertical Line for 16th Birthday Across All Tracks
  if(nrow(vdat) > 0) {
    p <- p + geom_vline(
      data = vdat,
      aes(xintercept = as.numeric(StartDate)),
      linetype = "dashed", linewidth = 1.2, color="black"
    ) 
    if(group=="Registration"){
      p <- p + geom_text(
            data = vdat %>% distinct(StartDate,Condition,.keep_all=TRUE) ,
            aes(x=StartDate,y=0,label=Condition),
            color="black",size=3,fontface="bold")
    }
  }

  p <- p + coord_cartesian(xlim = c(x_min, x_max))
  p <- p + scale_x_date(limits = c(x_min, x_max))

  # Use a minimal theme and remove y-axis elements.
  subTypes <- sub_df %>% select(SubType) %>% unique()
  if("Variable" %in% subTypes){
    p <- p + theme_minimal() +
      theme(
        plot.title = element_blank(),
        axis.text.x = element_text(size = 8)
      ) + labs(x = "Date")
  }else{
    p <- p + theme_minimal() +
      theme(
        axis.text.y = element_blank(),
        plot.title = element_blank(),
        axis.text.x = element_text(size = 8)
      ) + labs(x = "Date")
  }

  # Only show the x-axis on the bottom subplot.
  if (group != tail(levels(data$TrackName), 1)) {
    p <- p + theme(
      axis.text.x = element_blank(),
      axis.ticks.x = element_blank(),
      axis.title.x = element_blank()
    )
  }
  
  return(p)
}
