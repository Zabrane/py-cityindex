

function sma(data, n) {
    var sma = [];
    sma.length = data.length;

    var tot = 0;
    for(var i = 0; i < n; i++) {
        var val = data[i].Close;
        sma[i] = undefined;
        tot += val;
    }

    for(; i < data.length; i++) {
        sma[i] = tot / n;
        tot -= data[i-n].Close;
        tot += data[i].Close;
    }

    return sma;
}


function formatDate(d) {
    function zp(x)
    {
        return (x < 10) ? ("0" + x) : ("" + x);
    }

    d = new Date(d * 1000);
    var bits = [
        1900 + d.getYear(),
        '-',
        zp(1 + d.getMonth()),
        '-',
        zp(d.getDate()),
        ' ',
        zp(d.getHours()),
        ':',
        zp(d.getMinutes())
    ];
    return bits.join('');
}

var BCACHE = {};
var dygRows;

function deliverMsg(msg)
{
    if(msg[0] == 'price') {
        $('#price').text(JSON.stringify(msg[1], '    ', '    '));
    }
}

function drawGraph(data, _, __)
{
    addTime('barcount', data.length);
    BCACHE[window.market.MarketId] = data;

    addTime('bars', Date.now() - bars_t0);

    var t0 = Date.now();

    addTime('MyGraph', Date.now() - t0);

    var smaData = sma(data, 20);

    dygRows = [];
    for(var i = 0; i < data.length; i++) {
        var bar = data[i];
        dygRows.push([bar.BarDate, bar.Close, smaData[i]]);
    }

    t0 = Date.now();
    g.updateOptions({
        file: dygRows
    });
    addTime('DyGraph', Date.now() - t0)

    $.ajax('/subscribe?t=price&s=' + window.market.MarketId);
}


function onMarkets(data, textStatus, jqXHR)
{
    if(! data.length) {
        alert('no match');
        return;
    }

    if(window.market) {
        $.ajax('/unsubscribe?t=prices&s=' + window.market.MarketId);
    }

    window.market = data[0];
    $('#name').text(window.market.Name);
    //$('#market').text(JSON.stringify(window.market, '    ', '    '));
    var interval = $('input[name=interval]:checked').val();
    var bars = $('#bars').val();
    var span = $('#span').val();
    var key = window.market.MarketId + ':' +interval + ':' + bars + ':' + span;

    if(BCACHE[key]) {
        drawGraph(BCACHE[key]);
    }
    addTime('marketsearch', Date.now() - marketsearch_t0);
    bars_t0 = Date.now();
    $.ajax('/bars', {
        data: {
            s: window.market.MarketId,
            interval: $('input[name=interval]:checked').val(),
            bars: $('#bars').val(),
            span: $('#span').val()
        },
        success: drawGraph
    });
};


function addTime(s, t) {
    $('#time').append(s + ': ' + t + 'ms\n');
}

$(function()
{
    window.scr = $('<iframe>');
    window.scr.css({
        border: '0px none',
        width: '0px',
        height: '0px',
        display: 'none'
    });
    window.scr.attr('src', '/channel');
    window.scr.appendTo('body');

    g = new Dygraph($('#graph').get(0), dygRows, {
        height: 200,
        labels: ['Time', 'Close', 'SMA(20)'],
        xValueFormatter: function(g) { return formatDate(g); },
        xAxisLabelFormatter: function(g) { return formatDate(g); },
        yAxisLabelFormatter: function(g) { return String(g); },
    });

    $('form').submit(function()
    {
        $('#time').text('');
        marketsearch_t0 = Date.now();
        $.ajax('/markets', {
            data: $('form').serialize(),
            success: onMarkets
        });
        return false;
    });
});
