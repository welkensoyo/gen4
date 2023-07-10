%rebase('templates/render/datatablesedit.tpl', title='Velox Practices')
%columns = 'id','name','owner_name', 'address', 'address2', 'city', 'state', 'zip', 'phone', 'email', '3rd party', 'enabled', 'sync', 'created'
<div class="container-full">
    <div id="myAlert" class="alert alert-success alert-dismissible w-50 mx-auto" role="alert" style="display: none; position:fixed; left:25%; z-index:1; "> Information Saved </div>
    <h5 class="text-muted">&nbsp;&nbsp;&nbsp;{{name.upper()}}</h5>
<table id="tablex" class="display table-sm table-striped" width="100%"></table>
</div>
<script>
var dataSet = []
var wurl = ""

tablex = $('#tablex').DataTable( {
    dom: "<'row'<'col-md-3'<'toolbar'>><'col-md-3'f><'col-md-6'<'toolbar2'>>><'row'<'col-md-6'><'col-md-6'>><'row'<'col-md-12't>><'row'<'col-md-6'i><'col-md-6'p>>",
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


$.get(wurl, function( data ) {
  dataSet = data;
  tablex.destroy();
  tablex = $('#tablex').DataTable( {
    data: dataSet,
    dom: "<'row'<'col-md-3'<'toolbar'>><'col-md-3'f><'col-md-6'<'toolbar2'>>><'row'<'col-md-6'><'col-md-6'>><'row'<'col-md-12't>><'row'<'col-md-6'i><'col-md-6'p>>",
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
    $("div.toolbar").html('&nbsp;&nbsp;&nbsp;&nbsp;{{!'<button type="button" class="btn btn-sm btn-light pull-left" id="insert-more"><strong class="text-muted"> Add New Row </strong></button>'}}');
    $("div.toolbar2").html('<button type="submit"  class="btn btn-sm btn-light float-right border border-primary"><strong class="text-muted"> Save Changes</strong></button>&nbsp;&nbsp;&nbsp;&nbsp;');
    $('#tablex').editableTableWidget();
    $("#insert-more").click(function () {
         tablex.api().row.add({'label': 'EXAMPLE', 'mccCode': '0000', 'card_rate': '2.89', 'ach_rate': '1.0', 'crypto_rate': '1.99+5', 'card_p2c': '', 'ach_p2c': '', 'crypto_p2c': '', 'avg_ticket': '250', 'high_ticket': '10000'}).draw();
         $('#tablex').editableTableWidget();
     });
});
};

function submitHandler(){
    $('#tablex').DataTable().search('').draw(false);
    var tabledata = [];
    var $headers = $("th");
    var $rows = $("tbody tr").each(function(index) {
        $cells = $(this).find("td");
        tabledata[index] = {};
        $cells.each(function(cellIndex) {
            tabledata[index][$($headers[cellIndex]).html()] = $(this).html();
        });
    });
    const requestOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({'table': tabledata})
    };
    fetch('/api/practices', requestOptions).then()
    const myAlert = document.querySelector('#myAlert');
    myAlert.style.display = 'block';
    setTimeout(() => { myAlert.style.display = 'none';}, 3000);
};

$(document).ready(function() {
wurl = '/api/practices';
generate();
tablex.buttons().container().appendTo( $('.col-sm-6:eq(0)', tablex.table().container() ) );
} );
</script>