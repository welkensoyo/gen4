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
    <link rel="stylesheet" type="text/css" href="/static/css/all.min.css"/>
    <link rel="stylesheet" href="/static/css/app.css"/>
</head>
<body>
<div class="container">
    <div id="jeditor" class=""></div>
</div>
</body>
<script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js" integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo" crossorigin="anonymous"></script>
<script type="text/javascript" src="/static/js/jquery-3.6.3.min.js"></script>
<script type="text/javascript" src="/static/js/bootstrap.bundle.min.js"></script>
<script type="text/javascript" src="/static/js/ag-grid-community.min.js"></script>
<script type="text/javascript" src="/static/js/jsoneditor.js"></script>
<script>

const element = document.getElementById('jeditor');
let options = {
    theme:'bootstrap5',
    disable_edit_json:true,
    disable_properties:true,
    iconlib: "fontawesome5",
    schema: {{!schema}}
    }

const editor = new JSONEditor(element, options);
$( document ).ready(function() {
    editor.setValue({{!meta}})
})
</script>
</html>
</head>
