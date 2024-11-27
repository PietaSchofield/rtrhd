#' Plot a sankey network
#'
#' @export
plot_sankey <- function(datalist,sankeyFile,silent=T,db=F){
  if(db){
    sankeyFile <- file.path(.projLoc,"SLE_child_sankey.html")
    silent <- F
    datalist <- datalist
  }

  nodes <- datalist[["nodes"]]
  links <- datalist[["links"]]
  timeint <- datalist[["timeint"]]

  sankey <- networkD3::sankeyNetwork(
    Links = links, Nodes = nodes,
    Source = "source", Target = "target",
    Value = "value", NodeID = "name",
    units = "patients", fontSize = 30, 
    nodeWidth = 30,width=2400,height=800,
    nodePadding=50
  )

  interval_labels <- jsonlite::toJSON(timeint)
  title_text <- gsub("_"," ",gsub("[.]*$","",basename(sankeyFile)))
  idxline <- (length(timeint)-1)/2

  sankey <- sankey %>%
    htmlwidgets::onRender(sprintf("
      function(el, x) {
        // Interval labels from R variable
        var intervalLabels = %s;

        // Set approximate x-position for the diagnosis line
        // Define padding for top (for title) and bottom (for interval labels)
        var topPadding = 0;  // Adjust this value as needed
        var bottomPadding = 0;  // Adjust this as well
        var svg = d3.select(el).select('svg');
        var svgWidth = svg.node().getBoundingClientRect().width;
        var svgHeight = svg.node().getBoundingClientRect().height + topPadding + bottomPadding;
        
        // Calculate positions
        var n = intervalLabels.length;
        var spacing = svgWidth / n;
        
        // X-position for diagnosis line (adjust based on layout)
        var diagnosisX = (%f * spacing) + (spacing /2) ;  // diagnosis line pos
        // Add a title to the top of the SVG, centered

        svg.append('text')
           .attr('x', svgWidth / 2)          // Center horizontally
           .attr('y', 0)                    // Position at the top with some padding
           .attr('text-anchor', 'middle')    // Center alignment for the text
           .style('font-size', '24px')       // Font size for the title
           .style('font-weight', 'bold')     // Make it bold
           .text('%s');                      // Insert title text here

        // Append the diagnosis line
        svg.append('line')
          .attr('x1', diagnosisX)
          .attr('y1', 50)                 // Start at top of the diagram (adjust if needed)
          .attr('x2', diagnosisX)
          .attr('y2', svgHeight - 100)     // End at bottom of the diagram (adjust if needed)
          .attr('stroke-width', 2)
          .attr('stroke', 'black')
          .attr('stroke-dasharray', '5,5');  // Dashed line for diagnosis

        // Append interval labels with additional spacing below the Sankey diagram
        intervalLabels.forEach(function(label, i) {
          svg.append('text')
            .attr('x', (i + 0.5) * spacing)  // Center each label in its interval
            .attr('y', svgHeight - 30)       // Position below the diagram (adjust as needed)
            .style('font-size', '32px')
            .style('font-weight', 'bold')
            .style('text-anchor', 'middle')  // Center text
            .text(label);
        });
      }
    ", interval_labels, idxline, title_text))
 
  if(db){
    sankey
  }

  htmlwidgets::saveWidget(sankey, sankeyFile, selfcontained = TRUE, libdir="lib",
                          background="white",knitrOptions=list(css="svg{overflow: visible;}"))

  if(silent){
    return(sankeyFile)
  }else{
    if(db){
      displayURL(sankey)
    }
    return(sankey)
  }
}
