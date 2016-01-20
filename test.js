#!/usr/bin/nodejs

var http = require("http");
var url = require("url");
var fs = require("fs");
var os = require("os");
var child = require("child_process");

function logRequest(request) {
  console.log("%s %s %s",
              new Date().toISOString(),
              request.socket.remoteAddress, request.url);
}

function interfacesStats(ifaceNames, ifaceData, position, stats, callback) {
  if (position < ifaceNames.length) {
    var idata = {
      "IPv4" : new Array(),
      "IPv6" : new Array(),
      "rx" : 0,
      "tx" : 0,
    };
    var iface = ifaceNames[position];
    var hasAddresses = false;
    ifaceData[iface].filter(function(addr) {
      return ((addr.family == "IPv4") || (addr.family == "IPv6")) &&
             !addr.internal;
    }).forEach(function(addr) {
      idata[addr.family].push(addr.address);
      hasAddresses = true;
    });
    if (hasAddresses) {
      var path = "/sys/class/net/" + iface + "/statistics/";
      fs.readFile(path + "rx_bytes", "ascii", function(err, data) {
        idata["rx"] = data.replace(/\n$/, "");
        fs.readFile(path + "tx_bytes", "ascii", function(err, data) {
          idata["tx"] = data.replace(/\n$/, "");
          stats[iface] = idata;
          interfacesStats(ifaceNames, ifaceData, position + 1, stats, callback);
        });
      });
    } else {
      process.nextTick(function() {
        interfacesStats(ifaceNames, ifaceData, position + 1, stats, callback);
      });
    }
  } else {
    callback();
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

  logRequest(request);

  var path = url.parse(request.url);
  if (path.pathname == "/data") {
    var ifaces = os.networkInterfaces();
    var stats = {};
    interfacesStats(Object.keys(ifaces), ifaces, 0, stats, function() {
      child.exec("sleep 10; df -h", function(err, stdout, stderr) {
        var result = {
          "uptime"     : os.uptime(),
          "loadavg"    : os.loadavg(),
          "totalmem"   : os.totalmem(),
          "freemem"    : os.freemem(),
          "cpus"       : os.cpus(),
          "utc"        : new Date().getTime(),
          "diskfree"   : stdout,
          "interfaces" : stats,
        };
        response.writeHead(200);
        response.end(JSON.stringify(result));
      });
    });
  } else {
    response.writeHead(200);
    response.end("wurst");
  }
});
server.listen(8080);
