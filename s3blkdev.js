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

function parseDfOutput(result, stdout) {
  var lines = stdout.split(/\r?\n/);
  for (var i = 1; i < lines.length; i++) {
    var fields = lines[i].split(/\s+/);
    if (fields.length == 3) {
      Object.keys(result.dfree).forEach(function (d) {
        if (d == fields[0]) {
          result.dfree[d] = {
            "used" : fields[1],
            "avail": fields[2],
          };
        }
      });
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
      "hostname" : os.hostname(),
      "utc"      : new Date().getTime(),
      "devices"  : {},
      "dfree"    : {},
      "ifaces"   : {},
      "processes": 0,
    };

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
      var df = Object.keys(result.dfree).join(" ");
      child.exec("df -B1 -x tmpfs -x devtmpfs --output=file,used,avail " + df, queue.shift());
    }, function(err, stdout, stderr) {
      if (!err) {
        parseDfOutput(result, stdout);
      }
      child.exec("ps ax | grep s3blkdev-sync | grep -v grep | wc -l", queue.shift());
    }, function(err, stdout, stderr) {
      if (!err) {
        result.processes = stdout.split(/\r?\n/)[0];
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
server.listen(80);

var index_html = function (){/*
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">
<html>
<head>
<title>s3blkdev</title>
<style type="text/css">
body {
  font-family:sans-serif;
  padding:0;
  margin:0;
  background-color:Ivory;
}
h1 {
  position:fixed;
  text-align:center;
  width:100%;
  padding:0.2em 0 0.2em 0;
  margin:0;
  background-color:black;
  color:white;
}
h3 {
  margin-bottom:0;
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
  padding:0.1em 0em 0.1em 0em;
  margin:0;
  z-index:999;
}
#heartbeat {
  position:fixed;
  bottom:0;
  right:0;
  font-weight:bold;
  margin:0;
  color:red;
}
#sysinfo {
  position:absolute;
  top:4em;
  left:70%;
  width:29%;
}
#network {
  position:absolute;
  top:4em;
  left:1%;
  width:30%;
}
#devices {
  position:absolute;
  top:4em;
  left:31%;
  width:38%;
}
.barouter {
  margin:0;
  padding:0;
  height:0.5em;
  width:15em;
  border:0.1em inset black;
}
.barinner {
  height:0.5em;
}
.sub {
  margin-top:1em;
}
</style>
</head>
<body>
<h1 id="h1">s3blkdev</h1>

<div id="network">
<h2>Network</h2>
</div>

<div id="devices">
<h2>Devices</h2>
</div>

<div id="sysinfo">
<h2>Status</h2>
<h3>System Info</h3>
<div class="sub">Uptime:</div>
<div id="uptime"></div>
<div class="sub">Time:</div>
<div id="time"></div>
<div class="sub">Memory:</div>
<div class="barouter"><div id="memgraph" class="barinner" style="background-color:Chartreuse">&nbsp;</div></div>
<div id="memory"></div>
<div class="sub">CPUs:</div>
<div id="cpus"></div>
<div class="sub">Load average:</div>
<div id="loadavg"></div>
<h3 style="margin-top:2em">Services</h3>
<div class="sub">Processes:</div>
<div id="syncprocs"></div>
<div class="sub">Version:</div>
<div><a target="_blank" href="http://ogris.de/s3blkdev/">s3blkdev 0.6</a></div>
</div>

<div id="errorPane"></div>
<div id="errorText">Connection lost!</div>
<div id="heartbeat">&hearts;</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/1.0.2/Chart.js"></script>
<script type="text/javascript">
<!--
var netgraphs = {};
var devices = {};

function to_human (bytes) {
  var suffixes = ["B", "kB", "MB", "GB"];
  for (var i = 0; i < suffixes.length; i++) {
    if (bytes < 1000) break;
    bytes /= 1000;
  }
  return Math.round(bytes * 100)/100 + " " + suffixes[i] + "/s";
}

function to_human_ib (bytes) {
  var suffixes = ["B", "KiB", "MiB", "GiB", "TiB"];
  for (var i = 0; i < suffixes.length; i++) {
    if (bytes < 1024) break;
    bytes /= 1024;
  }
  return Math.round(bytes * 100)/100 + " " + suffixes[i];
}

function td (number) {
  return (number < 10 ? "0" + number : number);
}

function doughnut (data, device, offset, display) {
  var id = display + device;
  var mount = data.devices[device][offset];
  var container = document.getElementById(id);

  if (mount && data.dfree[mount]) {
    if (!container) {
      var container = document.createElement("div");
      container.id = id;
      document.getElementById("devices").appendChild(container);

      var div = document.createElement("div");
      div.id = "text" + id;
      container.appendChild(div);
      
      var canvas = document.createElement("canvas");
      canvas.id = "canvas" + id;
      container.appendChild(canvas);

      var chart = new Chart(canvas.getContext("2d")).Doughnut([{
        value: 0,
        color: "DarkCyan",
        highlight: "#109B9B",
        label: mount + " used",
      }, {
        value: 0,
        color: "DarkTurquoise",
        highlight: "#10DEE1",
        label: mount + " available",
      }], {
        tooltipTemplate: "<%if (label){%><%=label%>: <%}%><%= to_human_ib(value) %>",
        animationSteps: 50,
      });

      devices[id] = chart;
    }

    container.style.visibility = "visible";

    if ((devices[id].segments[0].value != data.dfree[mount].used) ||
        (devices[id].segments[1].value != data.dfree[mount].avail)) {
      devices[id].segments[0].value = data.dfree[mount].used;
      devices[id].segments[1].value = data.dfree[mount].avail;
      devices[id].update();

      var div = document.getElementById("text" + id);
      div.className = "sub";
      div.innerHTML = display + ": " + to_human_ib(data.dfree[mount].used) + " used, " +
                      to_human_ib(data.dfree[mount].avail) + " available";
    }
  } else if (container) {
    container.style.visibility = "hidden";
  }
}

function processData (response) {
  try {
    // system info
    var data = JSON.parse(response);

    // hostname
    document.title = "s3blkdev on " + data.hostname;
    document.getElementById("h1").innerHTML = "s3blkdev on "+ data.hostname;

    // uptime
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

    // time
    var now = new Date(data.utc);
    now = now.getFullYear() + "/" + td(now.getMonth() + 1) + "/" +
          td(now.getDate()) + " " + td(now.getHours()) + ":" +
          td(now.getMinutes()) + ":" + td(now.getSeconds());
    
    document.getElementById("time").innerHTML = now;

    // memory
    var memory = to_human_ib(data.freemem) + " of " +
                 to_human_ib(data.totalmem) + " free";
    document.getElementById("memory").innerHTML = memory;

    var memgraph = 15.0 * data.freemem / data.totalmem;
    document.getElementById("memgraph").style.width = memgraph + "em";

    // cpus
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

    // loadavg
    var loadavgs = []
    data.loadavg.forEach(function (c) {
      loadavgs.push(Math.round(c * 100) / 100);
    });
    document.getElementById("loadavg").innerHTML = loadavgs.join(" ");

    // sync processes
    document.getElementById("syncprocs").innerHTML = data.processes + " instances of s3blkdev-sync running";

    // network
    var maxspeed = 0;
    Object.keys(data.ifaces).forEach(function (iface) {
      if (netgraphs[iface]) {
        var deltatime = (data.utc - netgraphs[iface].utc) / 1000.0;
        if (deltatime > 0.0) {
          var speed = (data.ifaces[iface].rx - netgraphs[iface].rx) / deltatime;
          if (speed > maxspeed) maxspeed = speed;
          speed = (data.ifaces[iface].tx - netgraphs[iface].tx) / deltatime;
          if (speed > maxspeed) maxspeed = speed;
        }
      } else {
        netgraphs[iface] = {
          "utc": data.utc,
          "rx" : data.ifaces[iface].rx,
          "tx" : data.ifaces[iface].tx,
        };

        var h3 = document.createElement("h3");
        h3.id = "iface" + iface;
        h3.innerHTML = iface;
        document.getElementById("network").appendChild(h3);

        var div = document.createElement("div");
        div.innerHTML = "IP addresses:";
        div.className = "sub";
        document.getElementById("network").appendChild(div);

        div = document.createElement("div");
        div.id = "addr" + iface;
        document.getElementById("network").appendChild(div);

        div = document.createElement("div");
        div.innerHTML = "RX: ";
        div.className = "sub";
        document.getElementById("network").appendChild(div);

        span = document.createElement("span");
        span.id = "rxtext" + iface;
        div.appendChild(span);

        var barouter = document.createElement("div");
        barouter.className = "barouter";
        document.getElementById("network").appendChild(barouter);

        var barinner = document.createElement("div");
        barinner.id = "rxgraph" + iface;
        barinner.className = "barinner";
        barinner.style.backgroundColor = "Chartreuse";
        barouter.appendChild(barinner);

        div = document.createElement("div");
        div.innerHTML = "TX: ";
        div.className = "sub";
        document.getElementById("network").appendChild(div);

        span = document.createElement("span");
        span.id = "txtext" + iface;
        div.appendChild(span);

        barouter = document.createElement("div");
        barouter.className = "barouter";
        document.getElementById("network").appendChild(barouter);

        barinner = document.createElement("div");
        barinner.id = "txgraph" + iface;
        barinner.className = "barinner";
        barinner.style.backgroundColor = "DodgerBlue";
        barouter.appendChild(barinner);
      }
    });

    // network - maxspeed set, netgraphs filled
    Object.keys(data.ifaces).forEach(function (iface) {
      var deltatime = (data.utc - netgraphs[iface].utc) / 1000.0;

      if (deltatime > 0.0) {
        var speed = (data.ifaces[iface].rx - netgraphs[iface].rx) / deltatime;

        var div = document.getElementById("rxtext" + iface);
        div.innerHTML = to_human(speed);
        div = document.getElementById("rxgraph" + iface);
        div.style.width = (speed * 15.0 / maxspeed) + "em";

        speed = (data.ifaces[iface].tx - netgraphs[iface].tx) / deltatime;

        div = document.getElementById("txtext" + iface);
        div.innerHTML = to_human(speed);
        div = document.getElementById("txgraph" + iface);
        div.style.width = (speed * 15.0 / maxspeed) + "em";
      }

      netgraphs[iface].utc = data.utc;
      netgraphs[iface].rx = data.ifaces[iface].rx;
      netgraphs[iface].tx = data.ifaces[iface].tx;

      div = document.getElementById("addr" + iface);
      div.innerHTML = data.ifaces[iface].IPv4.join(", ");
      if (data.ifaces[iface].IPv6.length > 0) {
        div.innerHTML += ", " + data.ifaces[iface].IPv6.join(" ");
      }
    });

    // devices
    Object.keys(data.devices).forEach(function (device) {
      var h3 = document.getElementById("dev" + device);
      if (!h3) {
        h3 = document.createElement("h3");
        h3.id = "dev" + device;
        h3.innerHTML = device;
        document.getElementById("devices").appendChild(h3);
      }

      doughnut(data, device, 0, "Cache");
      doughnut(data, device, 1, "Filesystem");
    });
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
  var heartbeat = document.getElementById("heartbeat");
  heartbeat.style.visibility = (heartbeat.style.visibility == "hidden" ? "visible" : "hidden");
}

function startRequest () {
  toggleHeartBeat();

  var xmlHttp = new XMLHttpRequest();
  xmlHttp.timeout = 10000;
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
