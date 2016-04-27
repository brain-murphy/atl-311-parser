#!/Users/brian/.nvm/versions/node/v4.2.1/bin/node

var Parser = require('./atl-311-parser');

var parser = new Parser();

var zipCounts = {};


parser.onData(function (data) {
    if (!zipCounts[data.postalCode]) {
        zipCounts[data.postalCode] = 0;
    }
    
    zipCounts[data.postalCode] += 1;
});

parser.onFinished(function () {
    // console.log(zipCounts);
});

parser.start();