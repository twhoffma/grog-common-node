/*
var l = require('winston');

l.remove(l.transports.Console);
l.add(l.transports.Console, {colorize: true});
*/

fs = require('fs');
c = JSON.parse(fs.readFileSync('localconfig.json', 'utf8'));

var winston = require('winston');
var logger = new (winston.Logger)({
  levels: {
    trace: 9,
    input: 8,
    verbose: 7,
    prompt: 6,
    debug: 5,
    info: 4,
    data: 3,
    help: 2,
    warn: 1,
    error: 0
  },
  colors: {
    trace: 'magenta',
    input: 'grey',
    verbose: 'cyan',
    prompt: 'grey',
    debug: 'blue',
    info: 'green',
    data: 'grey',
    help: 'cyan',
    warn: 'yellow',
    error: 'red'
  }
});

var minLevel = (c.devmode ? 'debug': 'info');

logger.add(winston.transports.Console, {
  level: minLevel,
  prettyPrint: true,
  colorize: true,
  silent: false,
  timestamp: true
});

/*
logger.add(winston.transports.File, {
  prettyPrint: false,
  level: 'info',
  silent: false,
  colorize: true,
  timestamp: true,
  filename: './nKindler.log',
  maxsize: 40000,
  maxFiles: 10,
  json: false
});
*/

module.exports.info = logger.info;
module.exports.warn = logger.warn;
module.exports.error = logger.error;
module.exports.debug = logger.debug;
