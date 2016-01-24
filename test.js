#!/usr/bin/nodejs

var http = require("http");
var url = require("url");
var fs = require("fs");
var os = require("os");
var child = require("child_process");

function sendResponse(request, response, httpcode, contenttype, body) {
  process.nextTick(function() {
    response.writeHead(httpcode, {
      "Content-Type" : contenttype,
      "Content-Length" : body.length
    });
    response.end(body, function() {
      console.log("%s %s %d %d %s %s",
                  new Date().toISOString(),
                  request.socket.remoteAddress, httpcode,
                  body.length, request.method, request.url);
    });
  });
}

function interfacesStats(result, ifaceNames, ifaceData, position, callback) {
  if (position < ifaceNames.length) {
    var idata = {
      "IPv4" : [],
      "IPv6" : [],
      "rx" : 0,
      "tx" : 0,
    };
    var iface = ifaceNames[position];
    var path = "/sys/class/net/" + iface + "/statistics/";

    var queue = [function() {
      fs.readFile(path + "rx_bytes", "ascii", queue.shift());
    }, function(err, rx) {
      if (!err) {
        idata.rx = rx.replace(/\n$/, "");
        fs.readFile(path + "tx_bytes", "ascii", queue.shift());
      }
    }, function(err, tx) {
      if (!err) {
        idata.tx = tx.replace(/\n$/, "");
        result.ifaces[iface] = idata;
        interfacesStats(result, ifaceNames, ifaceData, position + 1, callback);
      }
    }];

    var hasAddresses = false;

    ifaceData[iface].filter(function(addr) {
      return ((addr.family == "IPv4") || (addr.family == "IPv6")) &&
             !addr.internal;
    }).forEach(function(addr) {
      idata[addr.family].push(addr.address);
      hasAddresses = true;
    });

    if (hasAddresses) {
      queue.shift()();
    } else {
      process.nextTick(function() {
        interfacesStats(result, ifaceNames, ifaceData, position + 1, callback);
      });
    }
  } else {
    callback();
  }
}

function parseS3BlkdevConf(result, s3blkdev_conf) {
  var device = null;
  var regres;

  s3blkdev_conf.split(/\r?\n/).forEach(function(line) {
    if ((regres = /^\[([^\]]+)/.exec(line))) {
      device = regres[1];
    } else if ((device !== null) && ((regres = /^cachedir\s+(.+)$/.exec(line)))) {
      var cachedir = regres[1];
      result.devices[device] = [ cachedir ];
      result.dfree[cachedir] = 0;
      device = null;
    } else if ((regres = /^listen\s+(.+)$/.exec(line))) {
      result["listen"] = regres[1];
    }
  });
}

function parseNbdClient(result, nbd_client) {
  var nbd = {};
  var regres;

  nbd_client.split(/\r?\n/).forEach(function(line) {
    if ((regres = /^NBD_DEVICE\[(\d+)\]\s*=\s*"?([^"]+)/.exec(line))) {
      var num = regres[1];
      var dev = regres[2];
      if (!nbd[num]) {
        nbd[num] = {};
      }
      nbd[num]["dev"] = dev;
    } else if ((regres = /^NBD_NAME\[(\d+)\]\s*=\s*"?([^"]+)/.exec(line))) {
      var num = regres[1];
      var device = regres[2];
      if (!nbd[num]) {
        nbd[num] = {};
      }
      nbd[num]["device"] = device;
    } else if ((regres = /^NBD_HOST\[(\d+)\]\s*=\s*"?([^"]+)/.exec(line))) {
      var num = regres[1];
      var host = regres[2];
      if (!nbd[num]) {
        nbd[num] = {};
      }
      nbd[num]["host"] = host;
     }
  });

  Object.keys(nbd).forEach(function(num) {
    if (nbd[num]["dev"] && nbd[num]["device"] && nbd[num]["host"]) {
      var device = nbd[num]["device"];
      var dev = nbd[num]["dev"];
      if (result.devices[device] && (result.listen == nbd[num]["host"])) {
        result.devices[device].push(dev);
        result.dfree[dev] = 0;
      }
    }
  });

  delete result.listen;
}

function parseDfOutput(result, df, stdout) {
  var lines = stdout.split(/\r?\n/);
  if (df.length + 1 == lines.length) {
    for (var i = 0; i < df.length; i++) {
      result.dfree[df[i]] = lines[1 + i].split(/\s+/);
    }
  }
}

var server = http.createServer(function(request, response) {
  request.on("error", function(err) {
    console.error(err);
    response.writeHead(400);
    response.end();
  });
  response.on("error", function(err) {
    console.error(err);
  });

  var path = url.parse(request.url);
  if (request.method != "GET") {
    sendResponse(request, response, 501, "text/plain",
                 "http method not supported");
  } else if (path.pathname == "/data") {
    var result = {
      "uptime"   : os.uptime(),
      "loadavg"  : os.loadavg(),
      "totalmem" : os.totalmem(),
      "freemem"  : os.freemem(),
      "cpus"     : os.cpus(),
      "utc"      : new Date().getTime(),
      "devices"  : {},
      "dfree"    : {},
      "ifaces"   : {},
    };

    var df;
    var queue = [function() {
      var ifaces = os.networkInterfaces();
      interfacesStats(result, Object.keys(ifaces), ifaces, 0, queue.shift());
    }, function() {
      fs.readFile("/usr/local/etc/s3blkdev.conf", "ascii", queue.shift());
    }, function(err, s3blkdev_conf) {
      if (!err) {
        parseS3BlkdevConf(result, s3blkdev_conf);
      }
      fs.readFile("/etc/nbd-client", "ascii", queue.shift());
    }, function(err, nbd_client) {
      if (!err) {
        parseNbdClient(result, nbd_client);
      }
      process.nextTick(queue.shift());
    }, function() {
      df = Object.keys(result.dfree);
      child.exec("df -B1 --output=size,used,avail " + df.join(" "), queue.shift());
    }, function(err, stdout, stderr) {
      if (!err) {
        parseDfOutput(result, df, stdout);
      }
      sendResponse(request, response, 200, "application/json", JSON.stringify(result));
    }];

    queue.shift()();
  } else if (path.pathname == "/favicon.ico") {
    sendResponse(request, response, 404, "image/x-icon", "");
  } else if (path.pathname == "/") {
    sendResponse(request, response, 200, "text/html", index_html);
  } else {
    sendResponse(request, response, 404, "text/plain", "file not found");
  }
});
server.listen(8080);

var index_html = function (){/*
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">
<html>
<head>
<title>ZeitMachine X</title>
<style type="text/css">
body {
  font-family:sans-serif;
  padding:0;
  margin:0;
}
h1 {
  text-align:center;
  border-bottom:1px solid black;
  width:100%;
  padding:10px 0 10px 0;
  margin:0;
  background-color:black;
  color:white;
}
#errorPane {
  width:100%;
  height:100%;
  position:absolute;
  top:0;
  left:0;
  opacity:0.5;
  background-color:#ccc;
  visibility:hidden;
  padding:0;
  margin:0;
  z-index:998;
}
#errorText {
  width:100%;
  position:absolute;
  top:50%;
  left:0;
  text-align:center;
  vertical-align:middle;
  opacity:1;
  background-color:red;
  visibility:hidden;
  color:white;
  font-weight:bold;
  font-size:large;
  padding:5px 0px 5px 0px;
  margin:0;
  z-index:999;
}
#heartBeat {
  position:fixed;
  bottom:0px;
  right:0px;
  font-weight:bold;
  margin:0;
}
#sysInfo {
  position:fixed;
  top:100px;
  right:150px;
}
</style>
</head>
<body>
<h1>ZeitMachine X</h1>
<div id="sysInfo">
<div>Uptime:</div>
<div id="uptime"></div>
<div>Time:</div>
<div id="time"></div>
<div>Memory:</div>
<div style="margin:0;padding:0;height:10px;width:100px;border:1px inset black"><div id="memgraph" style="height:10px">&nbsp;</div></div>
<div id="memory"></div>
<div>CPUs:</div>
<div id="cpus"></div>
<div>Load average:</div>
<div id="loadavg"></div>
</div>
<div id="errorPane"></div>
<div id="errorText">Connection lost!</div>
<div id="heartBeat">&hearts;</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/1.0.2/Chart.js"></script>
<script type="text/javascript">
<!--
function td (number) {
  return (number < 10 ? "0" + number : number);
}

function processData (response) {
  try {
    var data = JSON.parse(response);

    var uptime = data.uptime;
    var upsec = td(Math.floor(uptime % 60));
    uptime /= 60;
    var upmin = td(Math.floor(uptime % 60));
    uptime /= 60;
    var uphour = td(Math.floor(uptime % 24));
    uptime /= 24;
    var upday = Math.floor(uptime);

    uptime = upday + "d " + uphour + "h " + upmin + "m " + upsec + "s";
    document.getElementById("uptime").innerHTML = uptime;

    var now = new Date(data.utc);
    now = now.getFullYear() + "/" + td(now.getMonth() + 1) + "/" +
          td(now.getDate()) + " " + td(now.getHours()) + ":" +
          td(now.getMinutes()) + ":" + td(now.getSeconds());
    
    document.getElementById("time").innerHTML = now;

    var memory = Math.floor(data.freemem / (1024*1024)) + " of " +
                 Math.floor(data.totalmem / (1024*1024)) + " MiB free";
    document.getElementById("memory").innerHTML = memory;

    var memgraph = Math.floor(100 * data.freemem / data.totalmem);
    document.getElementById("memgraph").style.width = memgraph + "px";

    var green = Math.floor(512 * data.freemem / data.totalmem);
    var red = 512 - green;
    if (green > 255) green = 255;
    if (red > 255) red = 255;
    document.getElementById("memgraph").style.backgroundColor = "rgb(" + red + "," + green + ",0)";

    var cpumodels = {};
    for (var i = 0; i < data.cpus.length; i++) {
      cpumodels[data.cpus[i].model] = 0;
    }
    for (var i = 0; i < data.cpus.length; i++) {
      cpumodels[data.cpus[i].model]++;
    }
    var cpus = [];
    Object.keys(cpumodels).forEach(function (c) {
      cpus.push(cpumodels[c] + "x " + c);
    });
    document.getElementById("cpus").innerHTML = cpus.join("<br>");

    var loadavgs = []
    data.loadavg.forEach(function (c) {
      loadavgs.push(Math.round(c * 100) / 100 + 0.00);
    });
    document.getElementById("loadavg").innerHTML = loadavgs.join(" ");
  } catch (e) {
    toggleErrorPane("visible");
  }
}

function toggleErrorPane (visibility) {
  var errorPane = document.getElementById("errorPane");

  if (errorPane.style.visibility != visibility) {
    errorPane.style.visibility = visibility;
    document.getElementById("errorText").style.visibility = visibility;
  }
}

function toggleHeartBeat () {
  var heartbeat = document.getElementById("heartBeat");
  heartbeat.style.color = (heartbeat.style.color == "red" ? "gray" : "red");
}

function startRequest () {
  toggleHeartBeat();

  var xmlHttp = new XMLHttpRequest();
  xmlHttp.timeout = 1000;
  xmlHttp.onreadystatechange = function () {
    if (xmlHttp.readyState == 4) {
      if (xmlHttp.status == 200) {
        toggleErrorPane("hidden");
        processData(xmlHttp.response);
      } else {
        toggleErrorPane("visible");
      }
      window.setTimeout(startRequest, 1000);
    }
  };
  xmlHttp.open("GET", "/data");
  xmlHttp.send();
}

startRequest();
-->
</script>
</body>
</html>
*/}.toString().slice(15,-4);
/*
var memctx = document.getElementById("memory").getContext("2d");
var memchart = new Chart(memctx).Bar({
  labels: [],
  datasets: [{
    label:"Total",
    fillColor:"blue",
    strokeColor:"blue",
    highlightFill:"blue",
    highlightStroke:"blue",
    data:[0]
  }, {
    label:"Free",
    fillColor:"green",
    strokeColor:"green",
    highlightFill:"green",
    highlightStroke:"green",
    data:[0]
  }],
}, {
  scaleShowGridLines: false,
  scaleShowHorizontalLines: false,
  scaleShowVerticalLines: false,
  barShowStroke: false,
  barDatasetSpacing: 0,
  scaleFontFamily: "sans-serif",
  maintainAspectRatio: false,
  animation: false,
});

    memchart.datasets[0].bars[0].value = data.totalmem / (1024*1024*1024);
    memchart.datasets[1].bars[0].value = data.freemem / (1024*1024*1024);
    memchart.update();

*/
