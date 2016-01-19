#!/usr/bin/nodejs

var http = require("http");
var url = require("url");
var fs = require("fs");
var os = require("os");

const SYS_CLASS_NET = "/sys/class/net";

function logRequest(request) {
  console.log("%s %s %s",
              new Date().toISOString(),
              request.socket.remoteAddress, request.url);
}

function getInterfaceCounter(iface, counter) {
  var path = SYS_CLASS_NET + "/" + iface + "/statistics/" + counter + "_bytes";
  var bytes = fs.readFileSync(path, "ascii");
  bytes = bytes.replace(/\n$/, "");
  return bytes;
}

function getInterfacesStats() {
  var stats = {};
  fs.readdirSync(SYS_CLASS_NET).forEach(function(iface) {
    if (iface.charAt(0) == 'e') try {
      var rx = getInterfaceCounter(iface, "rx");
      var tx = getInterfaceCounter(iface, "tx");
      stats[iface] = { "rx" : rx, "tx" : tx };
    } catch (ex) {
    }
  });
  return JSON.stringify(stats);
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
    response.writeHead(200);
    response.end(getInterfacesStats());
  } else {
    response.writeHead(200);
    response.end("wurst");
  }
});
server.listen(8080);
