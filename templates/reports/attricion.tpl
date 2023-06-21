%rebase('templates/render/datatables.tpl', title='Users Removed from AD')
%columns = 'UserName', 'Deleted Date'
<div class="container">
    <h5 class="text-muted">Removed Users from AD</h5>
<table id="tablex" class="display table-sm table-striped" width="100%"></table>
</div>
<script>
var dataSet = []
var wurl = ""

tablex = $('#tablex').DataTable( {
    dom: '<"container-fluid"<"row"<"col"l><"col"f>rti<"col"B><"col"p>>>',
    select: false,
    responsive: false,
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


function generate() {
$.get(wurl, function( data ) {
  dataSet = data;
  tablex.destroy();
  tablex = $('#tablex').DataTable( {
    data: dataSet,
    dom: '<"container-fluid"<"row"<"col"l><"col"f>rti<"col"B><"col"p>>>',
    select: false,
    responsive: false,
    stateSave: false,
    colReorder: true,
    buttons: ['copy', 'excel',  'colvis' ],
    deferRender: true,
    "oLanguage": {"sSearch": "Filter:"},
    columns: [
%for column in columns:
        { title: "{{column.upper()}}" },
%end
    ],
    } );
    $('#loadingmodal').modal('hide')

});
};

$(document).ready(function() {
wurl = '/api/removed_ad_users';
generate();
tablex.buttons().container().appendTo( $('.col-sm-6:eq(0)', tablex.table().container() ) );
} );

</script>
