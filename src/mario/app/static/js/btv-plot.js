class BTVPlot {
    constructor(d3, options) {
        this.d3 = d3 || undefined;
        this.options = options || {
            replyRowHeight: 20
        }
        this.style = this.style(this.options);
    }
    style(options) {
        return {
            progress: {
                'color': '#222222',
                'width': '200px',
                'height': '20px',
                'text-align': 'center',
                'border': '1px dashed #888888',
                'padding': '15px',
                'left': '50%',
                'top': '50%',
                'position': 'absolute',
                'transform': 'translate(-50%, -50%)'
            },
            plot: {
                'font-family': 'sans-serif',
                'font-size': '13px',
                'border': '1px dashed #999999',
                'margin': '5px',
                'padding': '5px',
            },
            svg: {
                'margin': '0 auto',
                'display': 'block'
            },
            line: {
                'fill': 'url(#gradient)',
                'stroke': '#aaaaaa',
                'stroke-width': 1
            },
            axis: {
                'fill': 'none',
                'stroke': '#aaaaaa',
                'stroke-width': 1
            },
            text: {
                'fill': '#666666',
                'stroke': 'none'
            },
            circle: {
                'fill': '#4CFF00',
                'stroke': '#000000',
                'r': 4
            },
            overlay: {
                'fill': 'none',
                'pointer-events': 'all',
                'cursor': 'pointer'
            },
            replyListWrapper: {
                'width': '80%',
                'height': '350px',
                'margin': '5px auto',
                'overflow-x': 'hidden',
                'overflow-y': 'scroll'
            },
            replyList: {
                'font-family': 'sans-serif',
                'font-size': '13px',
                'position': 'relative'
            },
            replyToolTip: {
                'color': '#333333',
                'background-color': '#ffffff',
                'padding': '0px 8px',
                'display': 'none',
                'position': 'absolute'
            },
            reply: {
                'color': '#ffffff',
                'background-color': '#010101',
                'height': options.replyRowHeight + 'px',
                'line-height': options.replyRowHeight + 'px',
                'padding': '0 5px',
                'cursor': 'pointer'
            },
            replyTag: {
                'color': '#ffffff',
                'padding-right': '3px'
            },
            replyTagUp: {
                'color': '#00ff21'
            },
            replyTagDown: {
                'color': '#e01e24'
            },
            replyAuthor: {
                'width': '100px',
                'color': '#ffff66',
                'display': 'inline-block'
            },
            replyContent: {
                'color': '#999900'
            },
            replyDate: {
                'float': 'right'
            }
        }
    }
    render(container, viewerDataSource, temperatureDataSource, replyDataSource) {
        var progress = this.d3.select(container).append('div')
            .style(this.style.progress)
            .html('Rendering...');

        var viewerDataAccessor = d => {
            d.date = new Date(Date.parse(d.date_time.replace(/-/g, '/')));
            d.viewers = +d.total_concurrent_viewers;
            return d;
        }
        var temperatureDataAccessor = d => {
            d.date = new Date('2016/' + d.Var1);
            d.value = d['freq.normalized'];
            return d;
        }
        var replyDataAccessor = d => {
            d.Tag = d.notation.trim();
            d.date = new Date(Date.parse(d.ts.replace('T', ' ').replace(/-/g, '/')));
            d.dateString = 
                ('00' + (d.date.getMonth() + 1)).slice(-2) + '/' +
                ('00' + d.date.getDate()).slice(-2) + ' ' +
                ('00' + d.date.getHours()).slice(-2) + ':' +
                ('00' + d.date.getMinutes()).slice(-2);
            return d;
        }
        var objs = [
            { source: viewerDataSource, accessor: viewerDataAccessor },
            { source: temperatureDataSource, accessor: temperatureDataAccessor },
            { source: replyDataSource, accessor: replyDataAccessor }
        ];

        this.d3.csvPromiseAll(objs)
            .then(datas => {
                progress.remove();

                var self = this;
                
                var viewerData = datas[0];
                var minDate = this.d3.min(viewerData, d => d.date);
                var maxDate = this.d3.max(viewerData, d => d.date);
                var temperatureData = datas[1]
                    .filter(d => (d.date >= minDate && d.date <= maxDate));
                var replyData = datas[2]
                    .filter(d => (d.date >= minDate && d.date <= maxDate))
                    .sort((a, b) => (a.date < b.date ?  -1 : a.date > b.date ? 1 : 0));
                var replyDateMap = this.d3.map(replyData, d => d.date.getTime());
                
                var xAxisLabel = 'Time',
                    yAxisLabel = 'Viewers',
                    axisLabelOffset = 5,
                    margin = { left: 55, top: 20, right: 20, bottom: 50},
                    width = 1200 - margin.left - margin.right,
                    height = 250 - margin.top - margin.bottom;

                var plot = this.d3.select(container).append('div')
                    .style(this.style.plot)
                    .attr('width', width + margin.left + margin.right)
                    .attr('height', height + margin.top + margin.bottom);

                var replyListWrapper = this.d3.select(container).append('div')
                    .style(this.style.replyListWrapper);

                var replyList = replyListWrapper.append('div')
                    .style(this.style.replyList);

                var replyToolTip = replyList.append('p')
                    .style(this.style.replyToolTip);

                var svg = plot.append('svg')
                    .style(this.style.svg)
                    .attr('width', width + margin.left + margin.right)
                    .attr('height', height + margin.top + margin.bottom);

                var plotG = svg.append('g')
                    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

                var xScale = this.d3.time.scale()
                    .domain(this.d3.extent(viewerData.map(d => d.date)))
                    .range([0, width]);

                var yScale = this.d3.scale.linear()
                    .domain(this.d3.extent(viewerData.map(d => d.viewers)))
                    .range([height, 0]);

                var gradientColorScale = this.d3.scale.linear()
                    .domain([0, 1])
                    .range(['#FEFF00', '#FF0000']);

                var gradientOffsetScale = this.d3.scale.linear()
                    .domain(this.d3.extent(temperatureData.map(d => d.date)))
                    .range([0, 100]);

                var replyScale = this.d3.time.scale()
                    .domain(this.d3.extent(viewerData.map(d => d.date)))
                    .range([0, width]);

                var xAxis = this.d3.svg.axis()
                    .scale(xScale)
                    .ticks(20)
                    .orient('bottom');

                var yAxis = this.d3.svg.axis()
                    .scale(yScale)
                    .ticks(10)
                    .tickFormat(this.d3.format("s"))
                    .orient('left');

                var bisect = this.d3.bisector(d => d.date);

                var line = this.d3.svg.area()
                    .x(d => xScale(d.date))
                    .y0(height)
                    .y1(d => yScale(d.viewers))

                plotG.append("defs")
                    .append("linearGradient")
                    .attr("id", "gradient")
                    .attr("spreadMethod", "pad")
                    .selectAll('stop')
                    .data(temperatureData)
                    .enter()
                    .append('stop')
                    .attr("offset", d => (gradientOffsetScale(d.date) + '%'))
                    .attr("stop-color", d => gradientColorScale(d.value))
                    .attr("stop-opacity", 1);

                plotG.append('g')
                    .style(this.style.axis)
                    .attr('transform', 'translate(0, ' + height + ')')
                    .call(xAxis)
                    .selectAll('text')
                    .style(this.style.text);

                plotG.append('text')
                    .style(this.style.text)
                    .attr('x', width / 2)
                    .attr('y', height + margin.bottom - axisLabelOffset)
                    .attr('text-anchor', 'middle')
                    .text(xAxisLabel);

                plotG.append('g')
                    .style(this.style.axis)
                    .call(yAxis)
                    .selectAll('text')
                    .style(this.style.text);

                plotG.append('text')
                    .style(this.style.text)
                    .attr('transform', 'rotate(-90)')
                    .attr('y', -margin.left + axisLabelOffset * 3)
                    .attr('x', -height / 2)
                    .text(yAxisLabel);

                plotG.append('path')
                    .style(this.style.line)
                    .attr('d', line(viewerData));

                var circle = plotG.append('circle')
                    .style(this.style.circle)

                var overlay = plotG.append('rect')
                    .style(this.style.overlay)
                    .attr('width', width)
                    .attr('height', height);

                var onMouseMove = false;

                overlay
                    .on('click', function() {
                        var mousePosition = self.d3.mouse(this);
                        var x = mousePosition[0];
                        var date = xScale.invert(x);
                        var i = bisect.left(viewerData, date, 1);
                        var d1 = viewerData[i - 1];
                        var d2 = viewerData[i];
                        var d = date - d1.date > d2.date - date ? d2 : d1;
                        var replyIndex = replyData.map(d => d.date.getTime()).indexOf(d.date.getTime());
                        replyListWrapper[0][0].scrollTop = replyIndex * self.options.replyRowHeight;
                    })
                    .on('mouseout', function() {
                        onMouseMove = false;
                    })
                    .on('mousemove', function() {
                        onMouseMove = true;
                        var mousePosition = self.d3.mouse(this);
                        var x = mousePosition[0];
                        var date = xScale.invert(x);
                        var i = bisect.left(viewerData, date, 1);
                        var d1 = viewerData[i - 1];
                        var d2 = viewerData[i];
                        var d = date - d1.date > d2.date - date ? d2 : d1;
                        var y = yScale(d.viewers);
                        circle.moveTo(x, y);
                    });

                replyList
                    .on('mousemove', function() {
                        replyToolTip
                            .style({
                                'display': 'block'
                            })
                            .style({
                                left: (d3.mouse(this)[0] + 15) + 'px',
                                top: (d3.mouse(this)[1] + 5) + 'px',
                            });
                    })
                    .on('mouseout', function() {
                        replyToolTip.style({
                            'display': 'none'
                        });
                    })

                replyList.selectAll('div')
                    .data(replyData)
                    .enter()
                    .append('div')
                    .style(this.style.reply)
                    .attr('x', d => replyScale(d.date))
                    .attr('y', d => {
                        var i = bisect.left(viewerData, d.date, 1);
                        return viewerData[i] ? yScale(viewerData[i].viewers) : 0;
                    })
                    .on('mousemove', function(d) {
                        replyToolTip.html('來源文章: ' + d.topic)
                    })
                    .on('mouseover', function(d) {
                        var x = self.d3.select(this).attr('x');
                        var y = self.d3.select(this).attr('y');
                        self.d3.select(this).style('opacity', '.8')
                        circle.moveTo(x, y);
                    })
                    .on('mouseout', function() {
                        var x = self.d3.select(this).attr('x');
                        var y = self.d3.select(this).attr('y');
                        self.d3.select(this).style('opacity', '1')
                    });

                replyList.selectAll('div')
                    .append('span')
                    .style(this.style.replyTag)
                    .style({ 
                        color: d => 
                            this.style[
                                d.Tag == '推' ? 
                                    'replyTagUp' : d.Tag == '噓' ? 
                                        'replyTagDown' : 'replyTag'].color
                    })
                    .html(d => d.Tag);

                replyList.selectAll('div')
                    .append('span')
                    .style(this.style.replyAuthor)
                    .html(d => d.author);

                replyList.selectAll('div')
                    .append('span')
                    .style(this.style.replyContent)
                    .html(d => (':' + d.content));

                replyList.selectAll('div')
                    .append('span')
                    .style(this.style.replyDate)
                    .html(d => d.dateString);

                replyListWrapper[0][0].addEventListener('scroll', (e) => {
                    if(onMouseMove) return false;
                    var replyIndex = Math.round(e.target.scrollTop / this.options.replyRowHeight);
                    var reply = this.d3.select(replyList.selectAll('div')[0][replyIndex]);
                    var x = reply.attr('x');
                    var y = reply.attr('y');
                    circle.moveTo(x, y);
                });
            });
    }
}