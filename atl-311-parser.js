var fs = require('fs'),
    csv = require('fast-csv');
    
    
    function createCsvStream() {
        return csv()
        
        .on("data", function (data) {
            var dataObject = parse(data);
            
            onDataFunction(dataObject);
        })
        
        .on("end", function() {
            finishedFunction();
        });
    }
    
    function streamCsvData() {
        var stream = fs.createReadStream("ATL311 SR Data 2015.csv");
        stream.pipe(createCsvStream());
    }
    
    function parse(data) {
        var dataObject = {};
            dataObject['whenOpened'] = data[0];
            dataObject['source'] = data[1];
            dataObject['serviceRequestNumber'] = data[3];
            dataObject['knowledgeBaseArticleTopic'] = data[4];
            dataObject['department'] = data[5];
            dataObject['area'] = data[6];
            dataObject['subArea'] = data[7];
            dataObject['knowledgeBaseArticleId'] = data[8];
            dataObject['closedByName'] = data[9];
            dataObject['summary'] = data[10];
            dataObject['status'] = data[11];
            dataObject['substatus'] = data[12];
            dataObject['requestType'] = data[13];
            dataObject['expectedSlaResponseDays'] = data[14];
            dataObject['externalSystem'] = data[15];
            dataObject['hansenServiceRequestType'] = data[16];
            dataObject['externalSystemId'] = data[17];
            dataObject['externalSystemClosed'] = data[18];
            dataObject['GisLocation'] = data[19];
            dataObject['streetNumber'] = data[20];
            dataObject['streetName'] = data[21];
            dataObject['streetType'] = data[22];
            dataObject['cityCounty'] = data[23];
            dataObject['postalCode'] = data[24];
            dataObject['cityCouncilDistrictNumber'] = data[25];
            dataObject['incidentType'] = data[26];
            dataObject['primaryServiceRequestOnIncidentFlag'] = data[27];
            dataObject['proximityToIncident'] = data[28];
            dataObject['whenClosed'] = data[29];
            dataObject['priority'] = data[30];
            dataObject['closedByLogin'] = data[31];
            dataObject['contactBusinessName'] = data[32];
            return dataObject;
    }

    var onDataFunction = null;
    var finishedFunction = null;
    
 
module.exports = function () {
    this.onData = function (callback) {
        onDataFunction = callback;
    }
    
    this.onFinished = function (callback) {
        finishedFunction = callback;
    }
    
    this.start = function () {
        streamCsvData();
    }
}


////////////////////////////////////////////////////
// code used in the original project
////////////////////////////////////////////////////


/*
    geocoder = require('geocoder'),
    sleep = require('sleep');

var SUMMARY_COL_INDEX = 9,
    DEPARTMENT_COL_INDEX = 4,
    ZIP_COL_INDEX = 24,
    GIS_DATA_COL_INDEX = 19,
    TOPIC_COL_INDEX = 3,
    REJECTION_RATE = 0.98,
    TARGET_DATA_COUNT = 2500;
    
var dataGeocodedCount = 0;

var zipDict = {};

var waterOnlyZipDict = {};

var out = fs.createWriteStream('2500latlngs.txt');
 
    
    function rejectOrAcceptRandomly() {
        return Math.random() > REJECTION_RATE;
    }
    
    function geocodedDataIsValid(data) {
        return data && data.status === 'OK';
    }
    
    function geocode(data) {
        geocoder.geocode(data, function ( err, data ) {
            if (geocodedDataIsValid(data)) {
                try {
                    outputToFile(data);
                    
                    console.log('printed line ' + dataGeocodedCount + ' to file');
                    
                    sleep.usleep(100000);
                    
                } catch (err) {
                    console.log('error writing data:', err);
                    sleep.sleep(1);
                }
            } else {
                console.log('data not valid');
                sleep.sleep(1);
            }
        }, {key:"AIzaSyAKjQaeiE1HbHftEbYDqF5bedi7HS6HdUo"});
    }
    
    function outputToFile(data) {
        var latLngString = dataToLatLng(data);
                    
        out.write(latLngString);
                    
        dataGeocodedCount++;
    }
        
    function dataToLatLng(data) {
        return data.results[0].geometry.location.lat + ', ' + data.results[0].geometry.location.lng + '\n';
    }
    
    function filterAccounts(data) {
        return (data[TOPIC_COL_INDEX].toLowerCase().indexOf('water and sewer account') === -1) && (data[TOPIC_COL_INDEX].toLowerCase().indexOf('water and sewer bill') === -1);
    }
    
    function parseAddress(data) {
        return data[GIS_DATA_COL_INDEX + 1] + ' ' + data[GIS_DATA_COL_INDEX + 2] + ' ' + data[GIS_DATA_COL_INDEX + 3] + ', ' + data[GIS_DATA_COL_INDEX + 4] + ' GA ' + data[GIS_DATA_COL_INDEX + 5];
    }
    
    function addressFound(data) {
        return (data[GIS_DATA_COL_INDEX] && data[GIS_DATA_COL_INDEX].toLowerCase() === 'found');
    }
    
    function waterOnly(data) {
        if (data) {
            var lowerCaseSummary = data[SUMMARY_COL_INDEX];
            return lowerCaseSummary && ((lowerCaseSummary.indexOf('water') > -1) || (data[DEPARTMENT_COL_INDEX] && data[DEPARTMENT_COL_INDEX].indexOf('DWM') > -1) || (lowerCaseSummary.indexOf('sewer') > -1) || (lowerCaseSummary.indexOf('flood') > -1));
        }
    }
    
    function countZipcodes(data) {
        var zip = data[ZIP_COL_INDEX] || '';

        if (shouldUse(data)) {
            waterOnlyZipDict['total'] += 1;
            
            if (waterOnlyZipDict[zip]) {
                waterOnlyZipDict[zip] += 1;
            } else {
                waterOnlyZipDict[zip] = 1;;
            }
        }

        zipDict['total'] += 1;
        if (!validateZipcode(zip)) {
        // if (zip.indexOf('303') === -1) {
            return;
        }
        
        if (zipDict[zip]) {
            zipDict[zip] += 1;
        } else {
            zipDict[zip] = 1;;
        }
    }
    
    function validateZipcode(zipcode) {
        if (zipcode.length === 5 && !(isNaN(zipcode))) {
            return true;
        }
    }
    
    
    function printZips(zips) {
        var total = 0;
        for (var key in zips) {
            if (zips.hasOwnProperty(key)) {
                if (zips[key] > 2) {
                    total += zips[key];
                    console.log(key + ": " + zips[key]);
                } 
            }
        }
        // console.log('total: ' + total);
    }
    
    function printGraphHtml() {
        var BAR_HTML_TEMPLATE = '<div class="bar-segment" style="height:HEIGHT_PERCENT%;background-color:COLOR;">ZIP_CODE</div>',
            HEIGHT_PERCENT_TOKEN = 'HEIGHT_PERCENT',
            COLOR_TOKEN = "COLOR";
            ZIP_TOKEN = "ZIP_CODE";
            
        var topCountZips = getTopCounts(waterOnlyZipDict, 10);
        
        var waterOnlyDataCount = countDictValues(waterOnlyZipDict);
        
        var allDataCount = countDictValues(zipDict);
        
        var waterOnlyZipPercentages = {};
        
        topCountZips.forEach(function (val, index, array) {
            waterOnlyZipPercentages[val] = waterOnlyZipDict[val] / waterOnlyDataCount;
        });
        
        waterOnlyZipPercentages['other'] = 1 - countDictValues(waterOnlyZipPercentages);
        
        var allDataZipPercentages = {};
        
        topCountZips.forEach(function (val, index, array) {
            allDataZipPercentages[val] = zipDict[val] / allDataCount;
        });
        
        allDataZipPercentages['other'] = 1 - countDictValues(allDataZipPercentages);
        
        console.log('waterOnly percentages:', waterOnlyZipPercentages);
        console.log('total percentages:', allDataZipPercentages);
        
        var differences = {};
        
        for (var key in waterOnlyZipPercentages) {
            if (waterOnlyZipPercentages.hasOwnProperty(key) && allDataZipPercentages.hasOwnProperty(key)) {
                differences[key] = waterOnlyZipPercentages[key] - allDataZipPercentages[key];
                // console.log(key, differences[key]);
            }
        }
        
        var waterBarHtml = '';
        var totalBarHtml = '';
        
        
        var getAColor = (function () {
            var colors = ['#ffc0cb','#ff0000','#008080','#0000ff','#ffa500', '#800080','#ffc3a0','#00ff7f'];
            var nextColorIndex = 0;
            return function () {
                var colorToReturn = colors[nextColorIndex];
                nextColorIndex = (nextColorIndex + 1) % colors.length;
                return colorToReturn;
            }
        })();
        
        
        for (var key in waterOnlyZipPercentages) {
            if (waterOnlyZipPercentages.hasOwnProperty(key) && allDataZipPercentages.hasOwnProperty(key)) {
                var color = getAColor();
                waterBarHtml += fillBarTemplate(color, waterOnlyZipPercentages[key] * 100, key) + '\n';
                totalBarHtml += fillBarTemplate(color, allDataZipPercentages[key] * 100, key) + '\n';
            }
        }
        
        // console.log('water:\n', waterBarHtml);
        // console.log('all:\n', totalBarHtml);
        
       
        
        function fillBarTemplate(color, percent, zipcode) {
            return BAR_HTML_TEMPLATE
                .replace(COLOR_TOKEN, color)
                .replace(HEIGHT_PERCENT_TOKEN, percent)
                // .replace(ZIP_TOKEN, '');
                .replace(ZIP_TOKEN, zipcode);
        }
        
        function countDictValues(dict) {
            var total = 0;
            for (var key in dict) {
                if (dict.hasOwnProperty(key) && (!isNaN(dict[key]))) {
                    total += dict[key];
                }
            }
            return total;
        }
        
    }
    
    function getTopCounts(dict, numCounts) {
        var lowestCountKept = -1;
        var highestCounts = initArrayWithValue(numCounts, -1);
        var highestCountsKeys = initArrayWithValue(numCounts, '');
        for (var key in dict) {
            if (dict.hasOwnProperty(key) && dict[key] > lowestCountKept) {
                var indexReplacing = highestCounts.indexOf(lowestCountKept);
                highestCounts.splice(indexReplacing, 1, dict[key]);
                highestCountsKeys.splice(indexReplacing, 1, key);
                
                lowestCountKept = findLowestCountKept(highestCounts);
            }
        }
        
        function findLowestCountKept(counts) {
            var lowest = 100000;
            counts.forEach(function (ele, index, array) {
                if (ele < lowest) {
                    lowest = ele;
                }
            });
            return lowest;
        }
        
        function initArrayWithValue(numElements, value) {
            var arr = [];
            for (var i = 0; i < numElements; i++) {
                arr.push(value);
            }
            return arr;
        }
        
        return highestCountsKeys;
    }
    
    zipDict['total'] = 0;
    */