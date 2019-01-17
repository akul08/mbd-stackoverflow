function titleCase(str) {
    str = str.toLowerCase()
             .split(' ')
             .map(function(word) {
                    return word !== "and" ? (word.charAt(0).toUpperCase() + word.slice(1)) : word;
                });
   return str.join(' ');
  }

function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
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
                'subject': titleCase(row['subject'].replace("Summary", "").replace(' ', '_')),
                'scoreType' : row['popularity_measure'],
                'year' : parseInt(row["year"]),
                'lon' : parseInt(lon),
                'lat' : parseInt(lat),
                'value' : parseInt(row['value']),
                // 'radius' : parseInt(row['value']),
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
        init();

        // Add the bubbles;
        updateBubbles(curSubject, curScoreType);
    });

    function scoreTypeChangeListener(d, i, e) {
        let newScoreType = d3.event.target.attributes[0].nodeValue;
        curScoreType = newScoreType;
        updateBubbles(curSubject, curScoreType);
        // console.log(curSubject, curScoreType);
        d3.select("#filter p.scoreType").text("Score type: " + curScoreType);
    }

    function subjectChangeListener(d, i, e) {
        let newSubject = d3.event.target.attributes[0].nodeValue;
        curSubject = newSubject;
        updateBubbles(curSubject, curScoreType);
        // console.log(curSubject, curScoreType);

        d3.select("#filter p.subject").text("Subject: " + curSubject.replace('_', ' '));
        // console.log(d3.select("#filter p.subject"));
    }

    function createScoreTypeBtns(scoretypes) {
        d3.select("#filter").append('p').classed("scoreType", true).text("Score type: " + curScoreType);
        scoreTypeBtns = d3.select("#score_types");
        scoretypes.forEach( type => {
            scoreTypeBtns.append("button")
                        .attr("id", type)
                        .attr("class", "btn btn-default navbar-btn")
                        .text(type)
                        .on("click", scoreTypeChangeListener)
        });

        scoreTypeBtns.append("button")
                        .attr("id", "All")
                        .attr("class", "btn btn-default navbar-btn")
                        .text("All")
                        .on("click", scoreTypeChangeListener)
    }

    function createSubjectBtns(subjects) {
        d3.select("#filter").append('p').classed("subject", true).text("Subject: " + curSubject.replace('_', ' '));
        subjectBtns = d3.select("#subject_btns");

        subjects.forEach( subject => {
            subjectBtns.append("button")
                        .attr("id", subject)
                        .attr("class", "btn btn-default navbar-btn")
                        .text(subject.replace('_', ' '))
                        .on("click", subjectChangeListener)
        });

        subjectBtns.append("button")
                        .attr("id", "All")
                        .attr("class", "btn btn-default navbar-btn")
                        .text("All")
                        .on("click", subjectChangeListener)
    }

    // var series = [
    //     ["USA",36.2],["GBR",7.4],["CAN",6.2],["DEU",5.7],["FRA", 4.1],["ESP",4.1],["ITA",3.3],["MEX",3.0],["AUS",2.5],["NLD",2.4],
    //     ["IND",2.1],["BRA",2.0],["GRC",1.4],["AUT",1.2],["ROU",1.2],["SRB",1.0],["COL",0.8],["POL",0.8],["ZAF",0.7],["SWE",0.7],
    //     ["DNK",0.6],["VEN",0.6],["JPN",0.6],["KOR",0.6],["BEL",0.5],["RUS",0.5],["PRT",0.5]
    //                         ];

    // var dataset = {};

    // var onlyValues = series.map(function(obj){ return obj[1]; });
    // var minValue = Math.min.apply(null, onlyValues),
    //     maxValue = Math.max.apply(null, onlyValues);

    // var paletteScale = d3.scale.linear()
    //                             .domain([minValue,maxValue])
    //                             .range(["rgb(0,0,0)","rgb(219,219,219)"]);

    // series.forEach(function(item){ //
    //     // item example value ["USA", 36.2]
    //     var iso = item[0],
    //         value = item[1];
    //     dataset[iso] = { percent: value, fillColor: paletteScale(value) };
    // });

    globalRotation = [97, -30];
    globalZoom = 250;
    var globalTranslate = [500, 250];
    var globalRange = [2,6];
    var globalProjection = "orthographic";
    var scaleType = "relative";

    d3.select("#displayType").text(globalProjection)
                            .on("click", function() {
                                if (globalProjection === "orthographic") {
                                    globalProjection = "mercator";
                                } else {
                                    globalProjection = "orthographic";
                                }
                                d3.select("#displayType").text(globalProjection);
                                redraw();
                            });

    d3.select("#scaleType").text(scaleType)
                            .on("click", function() {
                                if (scaleType === "relative") {
                                    scaleType = "global";
                                } else {
                                    scaleType = "relative";
                                }
                                d3.select("#scaleType").text(scaleType);
                                redraw();
                                updateBubbles(curSubject, curScoreType);
                            });

    function init(){

        if (globalProjection === "orthographic") {
            drawOrthographic();
        } else {
            drawMercator();
        }

        map.legend({
            legendTitle : "Languages"
        });

        map.graticule();

        
        d3.select("#container").select("svg").append("defs").append("radialGradient")
                                            .attr("id", "ocean_fill")
                                            .attr("cx", "75%")
                                            .attr("cy", "25%");

        d3.select("#container").select("svg").on("mouseup", function() {updateBubbles(curSubject, curScoreType);})
    }

    function drawMercator() {
        map = new Datamap({
            element: document.getElementById("container"),
            scope: 'world',
            fills: getColorMappings(languages),
            geographyConfig: {
                highlightOnHover: false,
                responsive: true,       
                borderColor: 'rgba(222,222,222,0.2)',
                highlightBorderWidth: 1,
                borderWidth: 1,
                borderOpacity: 1,
                borderColor: '#C0C0C0',
            }, 
            projection : "mercator",
            done: function(datamap){
                datamap.svg.call(d3.behavior.zoom().on("zoom", redraw));
                function redraw() {
                    datamap.svg.selectAll("g").attr("transform", 
                                                    "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
                }
            }
        });
    }

    function drawOrthographic() {
        map = new Datamap({
            element: document.getElementById("container"),
            scope: 'world',
            fills: getColorMappings(languages),
            geographyConfig: {
                highlightOnHover: false,
                responsive: true,       
                borderColor: 'rgba(222,222,222,0.2)',
                highlightBorderWidth: 1,
                borderWidth: 1,
                borderOpacity: 1,
                borderColor: '#C0C0C0',
            }, 
            projection : "orthographic",
            projectionConfig : {
                rotation : globalRotation
            },
            // data: dataset,
            setProjection: function(element, options) {
                            var width = options.width || element.offsetWidth;
                            var height = options.height || element.offsetHeight;
                            var projection, path;
                            var svg = this.svg;
                        
                            projection = d3.geo[options.projection]()
                                .scale((width + 1) / 2 / Math.PI)
                                .translate([width / 2, height / (options.projection === "mercator" ? 1.45 : 1.8)]);

                            svg.append("defs").append("path")
                                .datum({type: "Sphere"})
                                .attr("id", "sphere")
                                .attr("d", path);

                            svg.append("use")
                                .attr("class", "stroke")
                                .attr("xlink:href", "#sphere");

                            svg.append("use")
                                .attr("class", "fill")
                                .attr("xlink:href", "#sphere");

                            projection.scale(globalZoom)
                                        .clipAngle(90)
                                        .rotate(options.projectionConfig.rotation)
                                        .translate(globalTranslate);

                            path = d3.geo.path().projection( projection );

                            return {path: path, projection: projection};
                        }
        });

        var zoom = d3.behavior.zoom().on("zoom", function(d, i) {
            
            scale = d3.event.scale;
            globalZoom = globalZoom * d3.event.scale;

            radiusZoom = 0.0005 * globalZoom;
            scaleRange = [2 + radiusZoom,6 + radiusZoom];
            setScaler(data, scaleRange);
            // console.log(d,i);
            // globalTranslate = computeTranslation()
            // else {
            //     scaleRange = [Math.max(2, 2 * 1.65 * scale),Math.max(6, 6 * 1.5 * scale)]
            //     setScaler(data, scaleRange);
            // }
            
            // map.svg.selectAll("g").attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");

            redraw();
            // updateBubbles(curSubject, curScoreType);
        });
           
        var drag = d3.behavior.drag().on('drag', function() {
            var dx = d3.event.dx;
            var dy = d3.event.dy;

            var rotation = map.projection.rotate();
            var radius = map.projection.scale();
            var scale = d3.scale.linear().domain([-1 * radius, radius]).range([-90, 90]);
            var degX = scale(dx);
            var degY = scale(dy);
            rotation[0] += degX;
            rotation[1] -= degY;
            if (rotation[1] > 90) rotation[1] = 90;
            if (rotation[1] < -90) rotation[1] = -90;

            if (rotation[0] >= 180) rotation[0] -= 360;
            globalRotation = rotation;
            redraw();
        })
        
        d3.select("#container").select("svg").call(drag);
        d3.select("#container").select("svg").call(zoom);
    }

    function computeTranslation(width, height) {

        if (d3.event == null || d3.event.translate == null) {
            return globalTranslate;
        }

        translate = d3.event.translate
        console.log(translate);

        dx = translate[0];
        dy = translate[1];

        globalTranslate[0] = 180 / width * dx;
        globalTranslate[1] = -180 / height * dy;

        return globalTranslate;
    }

    function redraw() {
        d3.select("#container").html('');
        init();
        // updateBubbles(curSubject, curScoreType);
    }

    function validCountry(country) {
        validValue = country !== "" && country !== "invalid";
        knownCountry = ccentroids[titleCase(country)] != null
        return validValue && knownCountry
    }

    function setColorMap(languages) {
        colorMap = d3.scale.ordinal()
                            .domain(languages)
                            .range(["#469990", "#000075", "#800000", "aqua", "#grey"]);
    }
    function setScaler(data, range=[2,25]) {
        radiusScaler = d3.scale.linear()
                            .domain(d3.extent(data, function(data) {
                                    return data.value}))
                            .range(range);
    }

    function getColorMappings(langs) {
        mapping = new Object();
        mapping['defaultFill'] = "#eee"; //"rgba(30,30,30,0.1)";

        langs.forEach(lang => {
            mapping[lang] = colorMap(lang);
        });
        return mapping;
    }

    // let yearyPlotsIntervalId;
    // function playYearlyPlots() {
    //     yearyPlotsIntervalId = setInterval(function() {
    //         console.log(year);
    //         year += 1;
    //         if (year == 2019) {
    //             year = 2012;
    //             addBubbles(year);
    //             clearInterval(yearyPlotsIntervalId);
    //         } else {
    //             addBubbles(year);
    //         }

    //         updateWorldYearIndicator();
    //     }, 1250);
    // }

    // function updateWorldYearIndicator() {
    //     d3.select("#worldYearIndicator").transition().delay(250).duration(1000).text("" + year);
    // }

    function getMapLoc(entry, radius) {
        loc = {};

        if (languages.indexOf(entry['language']) === 1) {
            loc['lat'] = entry['lat'] - 0.25 * radius;
            loc['lon'] = entry['lon'] + 0.25 * radius;
        } else if (languages.indexOf(entry['language']) === 2) {
            loc['lat'] = entry['lat'] + 0.25 * radius;
            loc['lon'] = entry['lon'] + 0.25 * radius;
        } else {
            loc['lat'] = entry['lat'];
            loc['lon'] = entry['lon'];
        }
        return loc
    }

    function getScoreWithSeps(...args) {
         // Sum results with equal language, subject & country
         values = new Object();
        //  console.log(values);

         data.forEach(function(current) {
             key = current['language'] + current['country'];

             args.forEach(function(elem){
                 key += '' + current[elem];
             });
    
             if (values[key] == null) {
                 values[key] = clone(current);
             } else {
                 values[key]['value'] += current['value'];
             }
         });
 
         return Object.values(values);
    }

    function getFilteredData(subject, scoreType) {

        if (scoreType === "All" && subject === "All") {
            return getScoreWithSeps();

        } else if (scoreType === "All") {
            return getScoreWithSeps("subject").filter(function(row) {
                return row['subject'] == subject;
            });
        } else if (subject === "All") { 
            return getScoreWithSeps("scoreType").filter(function(row) {
                return row['scoreType'] == scoreType;
            });
        } else {
            return getScoreWithSeps("subject", "scoreType").filter(function(row) {
                return row['scoreType'] === scoreType && row['subject'] === subject;
             });
        }
    }

    function updateBubbles(subject, scoreType) {
        all = getFilteredData('All', 'All');
        d = getFilteredData(subject, scoreType);
        if (scaleType === "global") {
            setScaler(all);
        } else {
            setScaler(d);   
        }

        bubbles = getBubbles(d);
        
        map.bubbles(bubbles, {
            popupTemplate: function(geo, data) {
              return '<div class="hoverinfo">' + 
                    data.country + ", Subject:  " + 
                    data.name + ", " +
                    data.significance + ''
            },
            borderWidth: .4
        
        });
    }

    function getBubbles(d) {

        let bubbles = d.map(function(row) {
            radius = radiusScaler(row['value']);
            country = row["country"];
            if (country === "United States") {
            }
            loc = getMapLoc(row, radius);

            name = curSubject

            // name = country + " " + row['subject'];
            // name = curSubject === "All" ? country + ", subject: " + curSubject : name ;

            return {
                'name': name,
                'radius' : radius,
                'country' : country,
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
