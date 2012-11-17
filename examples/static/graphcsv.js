
function now()
{
    return (new Date).getTime();
}


function readFileAsString(file, onComplete)
{
    var reader = new FileReader();
    reader.onload = function(e)
    {
        onComplete(e.target.result);
    };
    reader.readAsText(file);
}


function parseCsv(s)
{
    var t0 = (new Date).getTime();
    var rows = [];
    var lineRe = /([^\r\n]+)\r?\n/g;
    var colRe = /"?([^",]+)"?,?/g;

    var lineMatch, colMatch;

    while(lineMatch = lineRe.exec(s)) {
        var cols = [];
        colRe.lastIndex = 0;
        while(colMatch = colRe.exec(lineMatch[1])) {
            cols.push(colMatch[1]);
        }
        rows.push(cols);
    }

    return rows;
}


opts = {
    getX: function(rows, i) {
        var bits = rows[i][0].split('/');
        var d = new Date(bits[2], bits[0] - 1, bits[1]);
        return d.getTime();
    },
    getY: function(rows, i) {
        return +rows[i][1];
    },
    getOpen: function(rows, i) {
        return +rows[i][1];
    },
    getHigh: function(rows, i) {
        return +rows[i][2];
    },
    getLow: function(rows, i) {
        return +rows[i][3];
    },
    getClose: function(rows, i) {
        return +rows[i][4];
    }
};


function redraw()
{
    if(! window.rows) {
        return;
    }

    var t0 = now();
    window.lines.setData(rows);
    window.candles.setData(rows);
    $(window.candles.canvas).appendTo('body');
    window.chart.redraw();
    alert(now() - t0);
}

function onFileRead(s)
{
    window.rows = parseCsv(s);
    window.rows = window.rows.slice(rows.length - 198);
    redraw();
}


$('input').change(function()
{
    if(this.files.length) {
        readFileAsString(this.files[0], onFileRead);
    }
});

$(function()
{
    if(window.chart) {
        window.chart.remove();
    }
    window.chart = new MyChart('body', '100%', 320);
    window.candles = new CandleSeries(opts);
    window.lines = new LineSeries(opts);
    window.chart.addSeries(candles);

    $('button').click(redraw);
    var input = $('input').get(0);
    if(input.files.length) {
        readFileAsString(input.files[0], onFileRead);
    }
});
