
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


function convert(rows) {
    return rows.map(function(row)
    {
        var bits = row[0].split('/');
        var d = new Date(bits[2], bits[0] - 1, bits[1]);
        return [d.getTime(), +row[1], +row[2], +row[3], +row[4]];
    });
}


opts = {
    getX: function(rows, i) {
        return rows[i][0];
    },
    getY: function(rows, i) {
        return rows[i][1];
    },
    getOpen: function(rows, i) {
        return rows[i][1];
    },
    getHigh: function(rows, i) {
        return rows[i][2];
    },
    getLow: function(rows, i) {
        return rows[i][3];
    },
    getClose: function(rows, i) {
        return rows[i][4];
    }
};


function log()
{
    var args = Array.prototype.slice.apply(arguments);
    $('#log').append(args.toSource() + '\n');
}


function redraw()
{
    if(! window.rows) {
        return;
    }

    $('#log').text('');

    var t0 = now();
    window.candles.setData(rows);
    window.candleChart.redraw();
    log('Draw candles:', now() - t0);

    window.lines.setData(rows);
    t0 = now();
    window.lineChart.redraw();
    log('Draw lines:', now() - t0);
}

function onFileRead(s)
{
    window.rows = convert(parseCsv(s));
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
    window.lines = new LineSeries(opts);
    window.lineChart = new MyChart('#graphs', '100%', 150);
    window.lineChart.addSeries(window.lines);

    window.candles = new CandleSeries(opts);
    window.candleChart = new MyChart('#graphs', '100%', 150);
    window.candleChart.addSeries(window.candles);

    $('button').click(redraw);
    var input = $('input').get(0);
    if(input.files.length) {
        readFileAsString(input.files[0], onFileRead);
    }
});
