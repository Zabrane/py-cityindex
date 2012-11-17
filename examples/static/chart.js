

var Series = Base.extend({
    constructor: function(opts)
    {
        this.setOptions(opts);
        this.data = null;
    },

    setOption: function(name, value, default_)
    {
        if(value === undefined && this[name] === undefined) {
            this[name] = default_;
        } else if(value !== undefined) {
            this[name] = value;
        }
    },

    setOptions: function(opts)
    {
        this.setOption('width', opts.width, 320);
        this.setOption('height', opts.height, 200);
        this.setOption('getLength', opts.getLength, Series.getLength);
        this.setOption('getX', opts.getX, Series.getX);
        this.setOption('getY', opts.getY, Series.getY);
        this._initCanvas();
    },

    _initCanvas: function() {
        this.canvas = document.createElement('CANVAS');
        this.canvas.width = this.width;
        this.canvas.height = this.height;
        this.ctx = this.canvas.getContext('2d');
        // Hack to de-blur lines.
        this.ctx.translate(0.5, 0.5);
    },

    setData: function(data) {
        this.data = data;
    }
}, {
    getLength: function(data) {
        return data.length;
    },

    getX: function(data, i) {
        return data[i];
    },

    getY: function(data, i) {
        return i;
    }
});


var LineSeries = Series.extend({
    paint: function()
    {
        var data = this.data;
        var length = this.getLength(data);

        var getX = this.getX;
        var getY = this.getY;

        var yHigh = -Infinity;
        var yLow = Infinity;

        for(var i = 0; i < length; i++) {
            yHigh = Math.max(yHigh, getY(data, i));
            yLow = Math.min(yLow, getY(data, i));
        }

        var pad = (yHigh - yLow) * 0.1;
        yHigh += pad;
        yLow -= pad;

        var xLow = getX(data, 0);
        var xHigh = getX(data, length - 1);
        var pad = (xHigh - xLow) * 0.01;
        xHigh += pad;
        xLow -= pad;

        var xPerPt = this.width / (xHigh - xLow);
        var yPerPt = this.height / (yHigh - yLow);

        var ctx = this.ctx;
        ctx.moveTo((getX(data, 0) - xLow) * xPerPt,
          this.height - (getY(data, 0) - yLow) * yPerPt);

        for(var i = 1; i < length; i++) {
            ctx.lineTo((getX(data, i) - xLow) * xPerPt,
              this.height - (getY(data, i) - yLow) * yPerPt);
        }
        ctx.stroke();
    }
});


var OhlcSeries = Series.extend({
    setOptions: function(opts)
    {
        this.setOption('getX', opts.getX, OhlcSeries.getX);
        this.setOption('getOpen', opts.getOpen, OhlcSeries.getOpen);
        this.setOption('getHigh', opts.getHigh, OhlcSeries.getHigh);
        this.setOption('getLow', opts.getLow, OhlcSeries.getLow);
        this.setOption('getClose', opts.getClose, OhlcSeries.getClose);
        this.base(opts);
    }
}, {
    getX: function(data, i)
    {
        return data[i].BarDate;
    },

    getOpen: function(data, i)
    {
        return data[i].Open;
    },

    getHigh: function(data, i)
    {
        return data[i].High;
    },

    getLow: function(data, i)
    {
        return data[i].Low;
    },

    getClose: function(data, i)
    {
        return data[i].Close;
    }
});


var CandleSeries = OhlcSeries.extend({
    _getBarRange: function()
    {
        var getLow = this.getLow;
        var getHigh = this.getHigh;

        var data = this.data;
        var length = this.getLength(data);

        var low = Infinity;
        var high = -Infinity;

        for(var i = 0; i < length; i++) {
            low = Math.min(low, getLow(data, i));
            high = Math.max(high, getHigh(data, i));
        }

        return {
            low: low,
            high: high
        }
    },

    paint: function()
    {
        var data = this.data;
        var length = this.getLength(data);

        var yRange = this._getBarRange();
        var pad = (yRange.high - yRange.low) * 0.1;
        var yHigh = yRange.high + pad;
        var yLow = yRange.low - pad;

        var getOpen = this.getOpen,
            getHigh = this.getHigh,
            getLow = this.getLow,
            getClose = this.getClose;

        var xLow = this.getX(data, 0);
        var xHigh = this.getX(data, length - 1);
        var pad = (xHigh - xLow) * 0.01;
        xHigh += pad;
        xLow -= pad;

        var width = this.width;
        var height = this.height;

        var yPerPt = height / (yHigh - yLow);

        var barWidth = 4;
        var barMid = Math.ceil(barWidth / 2)
        var outerWidth = barWidth + 2;

        var ctx = this.ctx;
        var j = 0;
        ctx.strokeStyle = 'rgb(80, 80, 80)';
        for(i = length - 1; i; i--) {
            var barX = width - (outerWidth * ++j);
            if(barX < 0) {
                break;
            }

            var open = getOpen(data, i),
                high = getHigh(data, i),
                low = getLow(data, i),
                close = getClose(data, i);

            ctx.beginPath();
            ctx.moveTo(barX + barMid, height - ((high - yLow) * yPerPt));
            ctx.lineTo(barX + barMid, height - ((low - yLow) * yPerPt));
            ctx.closePath();
            ctx.stroke();

            if(open < close) {
                var biggie = close;
                var shortie = open;
            } else {
                var biggie = open;
                var shortie = close;
            }

            var relOpen = height - (biggie - yLow) * yPerPt;
            var relHeight = (biggie - shortie) * yPerPt;

            if(close < open) {
                ctx.fillStyle = 'rgb(255, 127, 127)';
                ctx.strokeStyle = 'rgb(127, 0, 0)';
                ctx.fillRect(barX, relOpen, barWidth, relHeight);
            } else {
                ctx.fillStyle = 'rgb(127, 255, 127)';
                ctx.strokeStyle = 'rgb(0, 127, 0)';
                ctx.fillRect(barX, relOpen, barWidth, relHeight);
            }
            ctx.strokeRect(barX, relOpen, barWidth, relHeight);
        }
    }
});


var MyChart = Base.extend({
    constructor: function(parent, width, height)
    {
        this.parent = parent;
        this.series = [];
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
        // this.ctx.translate(0.5, 0.5);
    },

    remove: function()
    {
        this.canvas.remove();
        this.canvas.empty();
    },

    addSeries: function(series)
    {
        series.setOptions({
            width: this.width - 50,
            height: this.height
        });
        this.series.push(series);
    },

    redraw: function()
    {
        this.ctx.clearRect(0, 0, this.width, this.height);
        for(var i = 0; i < this.series.length; i++) {
            var series = this.series[i];
            series.ctx.clearRect(0, 0, series.width, series.height);
            series.paint();
            this.ctx.drawImage(series.canvas, 50, 0);
        }
    }
});
