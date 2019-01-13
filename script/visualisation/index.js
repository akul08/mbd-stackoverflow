function titleCase(str) {
    str = str.toLowerCase()
             .split(' ') 
             .map(function(word) {
                    return word !== "and" ? (word.charAt(0).toUpperCase() + word.slice(1)) : word;
                });
   return str.join(' ');
  }


so_visualizer = function() {
    var year = 2012;
    var map;
    var ccentroids = new Object();
    var data;
    var radiusScaler;

    map = new Datamap({
        element: document.getElementById("container"),
        scope: 'world',
        // projection: 'orthographic',
        fills: {
        defaultFill: "#ABDDA4",
        win: '#0fa0fa'
        }, 
        projectionConfig: {
        rotation: [97,-30]
        }
    });

    d3.select("#worldYearIndicator").text("" + year);

    d3.select("#world_btn")
        .on("click", playYearlyPlots);

    d3.csv("country_centroids.csv", function(data){
        for (var i = 0; i < data.length; i++) {
            country = data[i]['name'];
            print(country)
            ccentroids[country] = {
                'lat' : data[i]['latitude'],
                'lon' : data[i]['longitude']
            }
        }
    });

    d3.csv("resultant_loc.csv", function(d) {
        
        clean_d = d.filter(function(row) {
            // if (ccentroids[titleCase(row['country'])] == null) {
            //     console.log(titleCase(row['country']))
            // }
            return row['country'] !== "" && 
                row['country'] !== "invalid" &&
                ccentroids[titleCase(row['country'])] != null
        }).map(function(row){
            country = titleCase(row['country'])
            lon = ccentroids[country].lon
            lat = ccentroids[country].lat
            return {
                'year' : parseInt(row["year"]),
                'lon' : lon,
                'lat' : lat,
                'value' : parseInt(row['value']),
                'country' : country
            }
        });

        values = new Object();

        for (var i = 0; i < clean_d.length; i++) {
            current = clean_d[i];
            country = current['country'];
            if (values[country] == null) {
                values[country] = current;
            } else {
                values[country]['value'] += current['value'];
            }
        }

        data = Object.values(values);
        setScaler();
        plotMap(year);
    });

    function setScaler() {
        radiusScaler = d3.scale.linear()
                            .domain(d3.extent(data, function(data) {
                                    return data.value}))
                            .range([2,12]);
    }

    let yearyPlotsIntervalId;
    function playYearlyPlots() {
        yearyPlotsIntervalId = setInterval(function() {
            console.log(year);
            year += 1;
            if (year == 2019) {
                year = 2012;
                plotMap(year);
                clearInterval(yearyPlotsIntervalId);
            } else {
                plotMap(year);
            }

            updateWorldYearIndicator();
        }, 1250);
    }

    function updateWorldYearIndicator() {
        d3.select("#worldYearIndicator").transition().delay(250).duration(1000).text("" + year);
    }
    // function replotMap(year) {
    //     d3.select("#container").selectAll("*").remove();
    //     plotMap(year);
    // }

    function plotMap(year) {
        d = data.filter(function(d){
            return d['year' ] === year
        });        

        bubbles = d.map(function(row) {
            radius = radiusScaler(row.value);
            country = row["country"]
            return {
                'name': country,
                'radius' : radius,
                'latitude' : row['lat'],
                'longitude' : row['lon'],
                'fillKey' : "win"
            }
        });
        map.bubbles(bubbles,  {strokeWidth: 3, arcSharpness: 0.7});
    }

    return {
        'plotMap' : plotMap
    }
}();
