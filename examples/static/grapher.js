
function now() {
    return (new Date).getTime();
}

var MyChart = Base.extend({
    constructor: function(parent, width, height)
    {
        this.parent = parent;
        this._initCanvas(width, height);
    },

    _initCanvas: function(width, height)
    {
        this.canvas = $(document.createElement('canvas'));
        this.canvas.appendTo(this.parent);

        this.canvas.css({
            width: width || '100%',
            height: height || '250px'
        });
        this.width = this.canvas.width();
        this.height = this.canvas.height();

        this.canvas.attr('width', this.width);
        this.canvas.attr('height', this.height);

        this.ctx = this.canvas[0].getContext('2d');
        // Hack to de-blur lines.
        this.ctx.translate(0.5, 0.5);
    },

    clear: function()
    {
        this.ctx.clearRect(0, 0, this.width, this.height);
    },

    remove: function()
    {
        this.canvas.remove();
        this.canvas.empty();
    },

    getBarRange: function(bars)
    {
        var low = Infinity;
        var high = -Infinity;

        for(var i = 0; i < bars.length; i++) {
            low = Math.min(low, bars[i].Low);
            high = Math.max(high, bars[i].High);
        }

        return {
            low: low,
            high: high
        }
    },

    setBars: function(bars, getXValue, getYValue)
    {
        var yHigh = -Infinity;
        var yLow = Infinity;

        for(var i = 0; i < bars.length; i++) {
            yHigh = Math.max(yHigh, getYValue(bars, i));
            yLow = Math.min(yLow, getYValue(bars, i));
        }

        var pad = (yHigh - yLow) * 0.1;
        yHigh += pad;
        yLow -= pad;

        var xLow = getXValue(bars, 0);
        var xHigh = getXValue(bars, bars.length - 1);
        var pad = (xHigh - xLow) * 0.01;
        xHigh += pad;
        xLow -= pad;

        var left = 50;

        var xPerPt = (this.width - left) / (xHigh - xLow);
        var yPerPt = this.height / (yHigh - yLow);

        var ctx = this.ctx;
        ctx.moveTo(left + ((getXValue(bars, 0) - xLow) * xPerPt),
          this.height - (getYValue(bars, 0) - yLow) * yPerPt);

        for(var i = 1; i < bars.length; i++) {
            ctx.lineTo(left + ((getXValue(bars, i) - xLow) * xPerPt),
              this.height - (getYValue(bars, i) - yLow) * yPerPt);
        }
        ctx.stroke();
    },

    setOhlcBars: function(bars, xStep)
    {
        var yRange = this.getBarRange(bars);
        var pad = (yRange.high - yRange.low) * 0.1;
        var yHigh = yRange.high + pad;
        var yLow = yRange.low - pad;

        var xLow = bars[0].BarDate;
        var xHigh = bars[bars.length - 1].BarDate;
        var pad = (xHigh - xLow) * 0.01;
        xHigh += pad;
        xLow -= pad;

        var width = this.width;
        var height = this.height;

        var left = 50;
        var yPerPt = height / (yHigh - yLow);

        var barWidth = 4;
        var barMid = Math.ceil(barWidth / 2)
        var outerWidth = barWidth + 2;

        var ctx = this.ctx;
        var j = 0;
        ctx.strokeStyle = 'rgb(80, 80, 80)';
        for(i = bars.length - 1; i; i--) {
            var barX = width - (outerWidth * ++j);
            if(barX < left) {
                break;
            }

            var bar = bars[i];

            ctx.beginPath();
            ctx.moveTo(barX + barMid, height - ((bar.High - yLow) * yPerPt));
            ctx.lineTo(barX + barMid, height - ((bar.Low - yLow) * yPerPt));
            ctx.closePath();
            ctx.stroke();

            if(bar.Open < bar.Close) {
                var biggie = bar.Close;
                var shortie = bar.Open;
            } else {
                var biggie = bar.Open;
                var shortie = bar.Close;
            }

            var relOpen = height - (biggie - yLow) * yPerPt;
            var relHeight = (biggie - shortie) * yPerPt;

            if(bar.Close < bar.Open) {
                ctx.fillStyle = 'rgb(255, 127, 127)';
                ctx.fillRect(barX, relOpen, barWidth, relHeight);
            } else {
                ctx.fillStyle = 'rgb(127, 255, 127)';
                ctx.fillRect(barX, relOpen, barWidth, relHeight);
            }
            ctx.strokeRect(barX, relOpen, barWidth, relHeight);
        }
    }
});


    var zp = function(x) { if (x<10) return "0"+x; else return ""+x;};

    function formatDate(d) {
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
var MIN_SPACE = 10;
var rows;

function deliverMsg(msg)
{
    if(msg[0] == 'price') {
        $('#price').text(JSON.stringify(msg[1], '    ', '    '));
    }
}

function getXValue(series, i) {
    return series[i].BarDate;
}
function getYValue(series, i) {
    return series[i].Close;
}


function sma(data, n, getYValue) {
    var sma = [];
    sma.length = data.length;

    var tot = 0;
    for(var i = 0; i < n; i++) {
        var val = getYValue(data, i);
        sma[i] = val;
        tot += val;
    }

    for(; i < data.length; i++) {
        sma[i] = tot / n;
        tot -= getYValue(data, i-n);
        tot += getYValue(data, i);
    }

    return sma;
}

function drawGraph(data, _, __)
{
    addTime('barcount', data.length);
    BCACHE[window.market.MarketId] = data;

    addTime('bars', now() - bars_t0);

    var t0 = now();

    var sm = sma(data, 60, getYValue);
    function getX(dat, i) { return getXValue(data, i); }
    function getY(dat, i) { return dat[i]; }

    window.meh.canvas.remove();
    window.meh.clear();
    window.meh.setOhlcBars(data, 60);
    //window.meh.setBars(data, getXValue, getYValue, 60);
    window.meh.setBars(sm, getX, getY, 60);
    addTime('MyGraph', now() - t0);
    window.meh.canvas.appendTo('#graph2');

    rows = [];
    for(var i = 0; i < data.length; i++) {
        var bar = data[i];
        rows.push([bar.BarDate, bar.Close, sm[i]]);
    }

    t0 = now();
    g.updateOptions({
        file: rows
    });
    addTime('DyGraph', now() - t0)

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
    addTime('marketsearch', now() - marketsearch_t0);
    bars_t0 = now();
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

    window.meh = new MyChart('#graph2');
    g = new Dygraph(document.getElementById('graph'), rows, {
        height: 200,
        labels: ['Time', 'Close', 'SMA'],
        xValueFormatter: function(g) { return formatDate(g); },
        xAxisLabelFormatter: function(g) { return formatDate(g); },
        yAxisLabelFormatter: function(g) { return String(g); },
    });

    $('form').submit(function()
    {
        $('#time').text('');
        marketsearch_t0 = now();
        $.ajax('/markets', {
            data: $('form').serialize(),
            success: onMarkets
        });
        return false;
    });
});
