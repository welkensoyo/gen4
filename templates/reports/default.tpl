<!DOCTYPE html>
<html lang="en">
<head>
    <title>SDB TEST</title>
    <meta charset="utf-8">
    <!--[if IE]><meta http-equiv='X-UA-Compatible' content='IE=edge,chrome=1'><![endif]-->
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="description" content="">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Roboto:400,700&subset=latin,cyrillic-ext">
    <link rel="stylesheet" type="text/css" href="/static/css/bootstrap.min.css"/>
    <link rel="stylesheet" type="text/css" href="/static/css/bootstrap-select.min.css"/>
    <link rel="stylesheet" href="/static/css/app.css"/>
</head>
<body>
<div id="sdbGrid" class="ag-theme-alpine" style="height: 500px"></div>
</body>
<script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js" integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo" crossorigin="anonymous"></script>
<script type="text/javascript" src="/static/js/jquery-3.6.3.min.js"></script>
<script type="text/javascript" src="/static/js/bootstrap.bundle.min.js"></script>
<script type="text/javascript" src="/static/js/ag-grid-community.min.js"></script>
<script>
const columns = [
  { field: "make",  resizable: true },
  { field: "model",  resizable: true },
  { field: "price",  resizable: true }
];

// specify the data
const data = [
  { make: "Toyota", model: "Celica", price: 35000 },
  { make: "Ford", model: "Mondeo", price: 32000 },
  { make: "Porsche", model: "Boxster", price: 72000 }
];

var grid = {
    // PROPERTIES
    // Objects like myRowData and myColDefs would be created in your application
    rowData: data,
    columnDefs: columns,
    pagination: true,
    rowSelection: 'single',

    // EVENTS
    // Add event handlers
    // onRowClicked: event => console.log('A row was clicked'),
    // onColumnResized: event => console.log('A column was resized'),
    // onGridReady: event => console.log('The grid is now ready'),

    // CALLBACKS
    getRowHeight: (params) => 25
}

document.addEventListener('DOMContentLoaded', () => {
    const gridDiv = document.querySelector('#sdbGrid');
    new agGrid.Grid(gridDiv, grid);
    grid.columnApi.autoSizeAllColumns();
    // gridOptions.columnApi.sizeColumnsToFit();
});


</script>
</html>
</head>
