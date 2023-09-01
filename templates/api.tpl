%rebase('templates/render/datatablesedit.tpl', title='API Command Center')
%columns = 'job', 'last run', 'error'
<div class="container">
    <h5 class="text-muted">&nbsp;&nbsp;&nbsp;Gen4 API</h5>
    <input id="hours" placeholder="Hours to Sync" value="8"></input><a class="btn btn-primary" id="syncb" onclick="sync()">Sync Velox</a>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a class="btn btn-primary" id="resetb" onclick="reset()">Full Reset</a>
<table id="tablex" class="display table-sm table-striped" width="100%"></table>
    <br>
    <br>
    <div id="message"><strong>Message: </strong> {{'Sync in progress...' if pause else 'No job currently running...' }}</div>
</div>
<script>
var logurl = '/api/velox/log?apikey={{apikey}}';

tablex = $('#tablex').DataTable( {
    dom: "<'row'<'col-md-3'<'toolbar'>><'col-md-3'><'col-md-6'<'toolbar2'>>><'row'<'col-md-6'><'col-md-6'>><'row'<'col-md-12't>><'row'<'col-md-6'i><'col-md-6'p>>",
    select: false,
    responsive: false,
    paging: false,
    stateSave: false,
    colReorder: true,
    buttons: ['copy', 'excel',  'colvis' ],
    deferRender: true,
    "oLanguage": {"sSearch": "Filter:"},
    columns: [
%for column in columns:
        { title: "{{column.upper()}}" },
%end
        ]
} );


$.get(logurl, function( data ) {
  tablex.destroy();
  tablex = $('#tablex').DataTable( {
    data: data,
    dom: "<'row'<'col-md-3'<'toolbar'>><'col-md-3'><'col-md-6'<'toolbar2'>>><'row'<'col-md-6'><'col-md-6'>><'row'<'col-md-12't>><'row'<'col-md-6'i><'col-md-6'p>>",
    select: false,
    responsive: false,
    paging: false,
    stateSave: true,
    colReorder: true,
    order: [[1, 'asc']],
    buttons: ['copy', 'excel',  'colvis' ],
    deferRender: true,
    oLanguage: {"sSearch": "Filter:"},
    columnDefs: [ ],
    columns: [
%for column in columns:
        { title: "{{column.upper()}}" },
%end
    ],
    } );
    $('#loadingmodal').modal('hide')

});

async function apiCall(url = "", data = {}) {
  const response = await fetch(url, {
    method: "POST", // *GET, POST, PUT, DELETE, etc.
    mode: "cors", // no-cors, *cors, same-origin
    cache: "no-cache", // *default, no-cache, reload, force-cache, only-if-cached
    credentials: "same-origin", // include, *same-origin, omit
    headers: {
      "Content-Type": "application/json",
    },
    referrerPolicy: "no-referrer", // no-referrer, *no-referrer-when-downgrade, origin, origin-when-cross-origin, same-origin, strict-origin, strict-origin-when-cross-origin, unsafe-url
    body: JSON.stringify(data), // body data type must match "Content-Type" header
  });
  return response.json(); // parses JSON response into native JavaScript objects
}

function sync() {
    const response = confirm("Are you sure you want to do that?");
        if (response) {
            document.getElementById('message').innerHTML = "<strong>Sync in progress...</strong> "
            hours = document.getElementById('hours').value
            apiCall('/api/velox/sync?apikey={{apikey}}&hours='+hours, {}).then((data) => {
                    document.getElementById('message').innerHTML = "<strong>Response:</strong> "+data
                });
        } else {
            console.log("Canceled");
        }

}
function reset() {
    const requestOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
    };
    const response = confirm("Are you sure you want to do that?");
        if (response) {
                document.getElementById('message').innerHTML = "<strong>Reset in progress...</strong> "
                apiCall('/api/velox/reset?apikey={{apikey}}', {}).then((data) => {
                    document.getElementById('message').innerHTML = "<strong>Response:</strong> "+data
                });
        } else {
            // add code if the user pressed the Cancel button
            console.log("Canceled");
        }

}

$(document).ready(function() {
    } );
</script>