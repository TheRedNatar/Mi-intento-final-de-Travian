import {DataTable, exportCSV} from "../vendor/simple-datatables"


  const position_x_input = document.querySelector("#position-x")
  const position_y_input = document.querySelector("#position-y")

  document.querySelector("#button-origin").addEventListener("click", () => {
      var params = new URLSearchParams(window.location.search); 
      params.set("position_x", position_x_input.value)
      params.set("position_y", position_y_input.value)
      var newUrl = window.location.origin 
          + window.location.pathname 
          + '?' + params.toString();
      window.location.assign(newUrl)
      }
  )


  let columnOptions = [
      {select: 0, type: "html"},
      {select: 1, type: "html", hidden: true},
      {select: 2, type: "html"},
      {select: 3, type: "html", hidden: true},
      {select: 4, type: "number"},
      {select: 5, type: "number"},
      {select: 6, type: "number"},
      {select: 7, type: "string", hidden: true},
      {select: 8, type: "string"},
      {select: 9, type: "string"},
      {select: 10, type: "number"}
  ]

  let options = {
      columns: columnOptions,
      perPageSelect: false,
      searchable: false,
      perPage: 30
  }

  const dataTable = new DataTable("#medusa_table", options)

  let columns = dataTable.columns

  const date = new Date()
  const [month, day, year] = [
      date.getMonth(),
      date.getDate(),
      date.getFullYear()]

  document.querySelector("#button-csv").addEventListener("click", () => {

      columns.show([1, 3, 7]);

      exportCSV(dataTable, {
          filename: `player_predictions_date_${year}_${month}_${day}_origin_${position_x_input.value}_${position_x_input.value}`,
          download: true,
          lineDelimiter: "\n"
      })

      columns.hide([1, 3, 7]);
  })


