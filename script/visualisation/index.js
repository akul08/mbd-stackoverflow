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
    var cur_subject;
    var map;
    var ccentroids = new Object();
    var data;
    var radiusScaler;
    var colorMap;
    var subjects;
    var languages;
    var scoreType = Question;

    d3.csv("country_centroids.csv", function(data){
        for (var i = 0; i < data.length; i++) {
            country = data[i]['name'];
            ccentroids[country] = {
                'lat' : data[i]['latitude'],
                'lon' : data[i]['longitude']
            }
        }
    });

    d3.csv("resultant_loc.csv", function(d) {
        
        clean_d = d.filter(function(row) {
            return validCountry(row['country']);

        }).map(function(row){
            country = titleCase(row['country'])
            lon = ccentroids[country].lon
            lat = ccentroids[country].lat
            return {
                'language' : row['language'],
                'subject': row['subject'],
                'year' : parseInt(row["year"]),
                'lon' : parseInt(lon),
                'lat' : parseInt(lat),
                'value' : parseInt(row['value']),
                'country' : country
            };
        });

        // Sum results with equal language, subject & country
        values = new Object();

        clean_d.forEach(function(current) {
            key = current['language'] + current['subject'] + current['country']

            if (values[key] == null) {
                values[key] = current;
            } else {
                values[key]['value'] += current['value'];
            }
        });

        data = Object.values(values);

        subjects = new Set(data.map(function(row){
            return row['subject'];
        }));

        languages = new Set(data.map(function(row){
            return row['language'];
        }));
        
        languages = Array.from(languages);
        

        setScaler();
        setColorMap(languages);

        drawMap();
        addBubbles();
    });

    function drawMap(){
        map = new Datamap({
                element: document.getElementById("container"),
                scope: 'world',
                // projection: 'orthographic',
                fills: getColorMappings(languages), 
                projectionConfig: {
                rotation: [97,-30]
            }
        });
        
    }

    function validCountry(country) {
        validValue = country !== "" && country !== "invalid";
        knownCountry = ccentroids[titleCase(country)] != null
        return validValue && knownCountry
    }

    function setColorMap(languages) {
        colorMap = d3.scale.ordinal()
                            .domain(languages)
                            .range(["#1b70fc", "#cb9b64", "#b21bff", "#7a7352", "#88fc07"]);
    }
    function setScaler() {
        radiusScaler = d3.scale.linear()
                            .domain(d3.extent(data, function(data) {
                                    return data.value}))
                            .range([2,100]);
    }

    function getColorMappings(langs) {
        mapping = new Object();
        mapping['defaultFill'] = "#ABDDA4";

        langs.forEach(lang => {
            mapping[lang] = colorMap(lang);
        });
        return mapping;
    }

    let yearyPlotsIntervalId;
    function playYearlyPlots() {
        yearyPlotsIntervalId = setInterval(function() {
            console.log(year);
            year += 1;
            if (year == 2019) {
                year = 2012;
                addBubbles(year);
                clearInterval(yearyPlotsIntervalId);
            } else {
                addBubbles(year);
            }

            updateWorldYearIndicator();
        }, 1250);
    }

    function updateWorldYearIndicator() {
        d3.select("#worldYearIndicator").transition().delay(250).duration(1000).text("" + year);
    }

    function getMapLoc(entry, radius) {
        loc = {};

        if (languages.indexOf(entry['language']) === 1) {
            console.log(languages.indexOf(entry['language']), entry['language'])
            loc['lat'] = entry['lat'] - radius;
            loc['lon'] = entry['lon'] + radius;
        } else if (languages.indexOf(entry['language']) === 2) {
            loc['lat'] = entry['lat'] + radius;
            loc['lon'] = entry['lon'] + radius;
        } else {
            loc['lat'] = entry['lat'];
            loc['lon'] = entry['lon'];
        }
        return loc
    }

    function addBubbles(year = null) {
        d = data.filter(function(row) {
            return row['subject'] === "Summarybigdata";
        });

        if (year !== null) {
            d = data.filter(function(d){
                return d['year' ] === year
            }); 
        }

        let bubbles = d.map(function(row) {
            radius = radiusScaler(row.value);
            country = row["country"];
            loc = getMapLoc(row, radius);

            return {
                'name': country,
                'radius' : radius,
                'fillKey' : row['language'],
                'latitude' : loc['lat'],
                'longitude' : loc['lon'],
                'significance' :  row['language'] + " has value " + row.value
            }
        });

        console.log(bubbles);
        map.bubbles(bubbles, {
            popupTemplate: function(geo, data) {
              return '<div class="hoverinfo">' + 
                    data.name + ": " + 
                    data.significance + ''
            }});
    }

    return {
        'addBubbles' : addBubbles
    }
}();
