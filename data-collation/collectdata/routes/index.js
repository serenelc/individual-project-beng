var express = require('express');
var router = express.Router();

const fetch = require('node-fetch');

let expectedArrivalTimes = ['']
let timeToLives = ['']
let timeStamps = ['']

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/collect', function(req, res, next) {
  // const stopId = req.query.stopId
  // const busId = req.query.busId
  const stopId = "490010536K"
  const busId = "452"

  fetch(`https://api.tfl.gov.uk/StopPoint/`+ stopId + `/arrivals`)
  .then(res => res.json())
  .then(json => {

    console.log(json[0])
    getInfoForSpecificBus(json, busId)
    const stationName = json[0].stationName
    const data = {expectedArrivalTimes, timeToLives, timeStamps, stationName, busId, title: 'Collecting Data'}

    res.render('index', data)
  })
  .catch(function(err) {
    console.log('Fetch Error :', err);
  })
})

function getInfoForSpecificBus(json, busNo) {

  json.forEach(info => {
    if (info.lineId === busNo) {
      expectedArrivalTimes.push(info.expectedArrival)
      timeToLives.push(info.timeToLive)
      timeStamps.push(info.timestamp)
    }
  });

}


module.exports = router;
