#!/usr/bin/nodejs

var http = require("http");
var url = require("url");
var fs = require("fs");
var os = require("os");
var child = require("child_process");
var util = require("util");

function logRequest(request) {
  console.log("%s %s %s",
              new Date().toISOString(),
              request.socket.remoteAddress, request.url);
}

function getInterfaceCounter(iface, counter) {
  var path = "/sys/class/net/" + iface + "/statistics/" + counter + "_bytes";
  var bytes = fs.readFileSync(path, "ascii");
  bytes = bytes.replace(/\n$/, "");
  return bytes;
}

function getInterfacesStats() {
  var interfaces = os.networkInterfaces();
  var stats = {};
  for (var iface in interfaces) {
    var idata = {
      "IPv4" : new Array(),
      "IPv6" : new Array(),
      "rx" : 0,
      "tx" : 0,
    };
    var hasAddresses = false;
    interfaces[iface].filter(function(addr) {
      return ((addr.family == "IPv4") || (addr.family == "IPv6")) &&
             !addr.internal;
    }).forEach(function(addr) {
      idata[addr.family].push(addr.address);
      hasAddresses = true;
    });
    if (!hasAddresses) continue;
    try {
      idata["rx"] = getInterfaceCounter(iface, "rx");
      idata["tx"] = getInterfaceCounter(iface, "tx");
      stats[iface] = idata;
    } catch (e) {
    }
  }
  return stats;
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
    var result = {
      "interfaces" : getInterfacesStats(),
      "uptime"     : os.uptime(),
      "loadavg"    : os.loadavg(),
      "totalmem"   : os.totalmem(),
      "freemem"    : os.freemem(),
      "cpus"       : os.cpus(),
      "utc"        : new Date().getTime(),
      "diskfree"   : "",
    };
    child.exec("sleep 10; ls /wurst", function(err, stdout, stderr) {
      result["diskfree"] = stdout;
      response.writeHead(200);
      response.end(JSON.stringify(result));
    });
  } else {
    response.writeHead(200);
    response.end("wurst");
  }
});
server.listen(8080);
