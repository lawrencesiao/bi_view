(function(d3) {
    if(d3) {
        d3.csvPromise = function(source, accessor) {
            return new Promise((resolve, reject) =>
                d3.csv(
                    source, 
                    accessor, 
                    (err, data) => (err ? reject({ error: err }) : resolve(data))
                )
            );
        }

        d3.csvPromiseAll = function(objs) {
            return Promise.all(objs.map(obj => d3.csvPromise(obj.source, obj.accessor)));
        }

        d3.selection.prototype.moveTo = function(x, y) {
            return this.attr('transform', 'translate(' + x + ', ' + y + ')');
        }

        d3.selection.prototype.bringToFront = function() {
            return this.each(function(){
                this.parentNode.appendChild(this);
            });
        }
    }
})(d3);
