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
      console.log("%s %s %d %d %s",
                  new Date().toISOString(),
                  request.socket.remoteAddress, httpcode,
                  body.length, request.url);
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
  if (path.pathname == "/data") {
    var ifaces = os.networkInterfaces();
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
    sendResponse(request, response, 200, "text/html", "<h1>wurst</h1>");
  } else {
    sendResponse(request, response, 404, "text/plain", "file not found");
  }
});
server.listen(8080);
