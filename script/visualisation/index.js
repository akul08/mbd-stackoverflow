function titleCase(str) {
    str = str.toLowerCase()
             .split(' ') 
             .map(function(word) {
                    return word !== "and" ? (word.charAt(0).toUpperCase() + word.slice(1)) : word;
                });
   return str.join(' ');
  }


so_visualizer = function() {
    let display_settings = {
        // Fill
    };
    var year = 2012;
    var map;
    var ccentroids = new Object();
    var data;
    var radiusScaler;
    var colorMap;
    var languages;
    var subjects;

    var curSubject = "All";
    var curScoreType = "All";

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
                'subject': row['subject'].replace(' ', ''),
                'scoreType' : row['popularity_measure'],
                'year' : parseInt(row["year"]),
                'lon' : parseInt(lon),
                'lat' : parseInt(lat),
                'value' : parseInt(row['value']),
                'country' : country
            };
        });
        data = clean_d;

        // Get unique subjects
        subjects = new Set(data.map(function(row){
            return row['subject'];
        }));
        subjects = Array.from(subjects);

        // Get unique languages
        languages = new Set(data.map(function(row){
            return row['language'];
        }));
        languages = Array.from(languages);

        // Get unique Score Types
        scoretypes = new Set(data.map(function(row){
            return row['scoreType'];
        }));
        scoretypes = Array.from(scoretypes);

        // Set default radius scaler & color mapping.
        setScaler(data);
        setColorMap(languages);

        // Add subject & score type btns
        createSubjectBtns(subjects);
        createScoreTypeBtns(scoretypes);

        // Draw the map with settings
        drawMap();

        // Add the bubbles;
        updateBubbles(curSubject, curScoreType);
    });

    function scoreTypeChangeListener(d, i, e) {
        let newScoreType = d3.event.target.attributes[0].nodeValue;
        curScoreType = newScoreType;
        updateBubbles(curSubject, curScoreType);
        console.log(curSubject, curScoreType);
    }

    function subjectChangeListener(d, i, e) {
        let newSubject = d3.event.target.attributes[0].nodeValue;
        curSubject = newSubject;
        updateBubbles(curSubject, curScoreType);
        console.log(curSubject, curScoreType);
    }

    function createScoreTypeBtns(scoretypes) {
        scoreTypeBtns = d3.select("#score_types");
        scoretypes.forEach( type => {
            scoreTypeBtns.append("button")
                        .attr('id', type)
                        .text(type)
                        .on("click", scoreTypeChangeListener)
        });

        scoreTypeBtns.append("button")
                        .attr('id', "All")
                        .text("All")
                        .on("click", scoreTypeChangeListener)
    }

    function createSubjectBtns(subjects) {
        subjectBtns = d3.select("#subject_btns");

        subjects.forEach( subject => {
            subjectBtns.append("button")
                        .attr('id', subject)
                        .text(subject)
                        .on("click", subjectChangeListener)
        });

        subjectBtns.append("button")
                        .attr('id', "All")
                        .text("All")
                        .on("click", subjectChangeListener)
    }

    function drawMap(){
        map = new Datamap({
                element: document.getElementById("container"),
                scope: 'world',
                projection: 'mercator',
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
    function setScaler(data) {
        radiusScaler = d3.scale.linear()
                            .domain(d3.extent(data, function(data) {
                                    return data.value}))
                            .range([2,10]);
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

    function getScoreSum() {
        // Sum results with equal language, subject & country
        values = new Object();

        data.forEach(function(current) {
            key = current['language'] + current['subject'] + current['country']

            if (values[key] == null) {
                values[key] = current;
            } else {
                values[key]['value'] += current['value'];
            }
        });

        return Object.values(values);
    }

    function getFilteredData(subject, scoreType) {
        let subsetD;

        if (scoreType === "All") {
            subsetD = getScoreSum();
            // Different scales. update scaler
        } else {
            subsetD = data.filter(function(row) {
                return row['scoreType'] === scoreType;
             });
        }

        if (subject === "All") {
            return subsetD;
        } else {
            return subsetD.filter(function(row) {
                return row['subject'] === subject;
            });
        }
    }

    function updateBubbles(subject, scoreType) {
        d = getFilteredData(subject, scoreType);
        setScaler(d);
        
        bubbles = getBubbles(d, subject);

        map.bubbles(bubbles, {
            popupTemplate: function(geo, data) {
              return '<div class="hoverinfo">' + 
                    data.name + ": " + 
                    data.significance + ''
            },
            borderWidth: .4
        
        });
    }

    function getBubbles(d, subject) {

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

        return bubbles;       
    }

    return {
        // 'addBubbles' : addBubbles
    }
}();
