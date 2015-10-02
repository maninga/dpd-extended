
/* global require:false, process:false */

var uuid = require('deployd/lib/util/uuid');
var createFunction = require('deployd/lib/util/create-function');

var extension = {};

var groupIds = extension.groupIds = [
  'group', 'sum', 'min', 'max', 'avg' /*,
  maybe later
  'population_variance', 'population_standard_deviation',
  'sample_variance', 'sample_standard_deviation'
  */
];

var suggarIds = extension.suggarIds = ['count', 'distinct', 'index-of'].concat(groupIds);

var isIdentifier = extension.isIdentifier = function isIdentifier(str) {
  'use strict';
  str = (str || '').toString();

  if (str.length === 24 && uuid.isObjectID(str)) {
    return true;
  } else if (str.length === 16 && /[a-z]/.test(str) && /[a-z]/.test(str) && /^[a-z0-9]+$/.test(str)) {
    return true;
  } else {
    return false;
  }
};

function extendConfig() {
  'use strict';
  /*
   * Extensions for 'deployd/lib/config-loader'
   *
   * cache config in development mode if options.cache_config is true
   *
   */
  (function(Config) {

    var debug = require('debug')('config-loader:extended');

    Config._loadConfig = Config.loadConfig;
    Config.loadConfig = function(basepath, server, fn) {
      if (server.options && server.options.env === 'development' && !!server.options.cache_config) {
        debug('WARNING: caching config in development env');
        server.options.env = 'dev';
        return Config._loadConfig(basepath, server, function(err, res){
          server.options.env = 'development';
          fn(err, res);
        });
      } else {
        return Config._loadConfig(basepath, server, fn);
      }
    };
  })(require('deployd/lib/config-loader'));
}

function extendScript() {
  'use strict';
  /*
   * Extensions for 'deployd/lib/script'
   *
   * add process() method to the domain that return the global process variable
   * add context() method to the domain that return the script execution context
   * add require(resource) method to the domain that proxies to global require
   *
   */
  (function(Script) {

    var _run = Script.prototype.run;
    Script.prototype.run = function(ctx, domain, fn) {
      if (typeof domain === 'object') {
        domain.process = function() {
          return process;
        };
        domain.context = function() { // access Context via context()
          return ctx;
        };
        domain.require = function(resource) {
          return require(resource);
        };
      }
      _run.call(this, ctx, domain, fn);
    };
  })(require('deployd/lib/script'));
}

function extendCollection() {
  'use strict';
  /*
   * Extensions for 'deployd/lib/resources/collection'
   *
   * monkeyPatch the Collection.prototype.count method so it is available to all requests
   * monkeyPatch the Collection.prototype.indexOf method so it is available to all requests
   * add Collection.prototype.distinct method to return distinct values for a given field
   * add Collection.prototype.group method to compute basic mapReduction for a given field: sum, min, max, avg
   * add Collection.prototype.sum method to pick sum property from Collection.prototype.group result
   * add Collection.prototype.min method to pick min property from Collection.prototype.group result
   * add Collection.prototype.max method to pick max property from Collection.prototype.group result
   * add Collection.prototype.avg method to pick avg property from Collection.prototype.group result
   * override Collection.prototype.handle method to take care of new methods (distinct, min, max)
   * override Collection.prototype.sanitize to handle dot.notation in request body (for POST and PUT[ and DEL?])
   * override Collection.prototype.sanitizeQuery to handle dot.notation in request query
   * override Collection.prototype.execCommands to
   *  - handle global commands (e.g $set{'deep.object.prop1': prop1Val, 'deep.object.prop2.sub1': prop2Sub1Val })
   *  - handle dot.notation in operators
   *  - handle more fields operator (thanks to mongodb fields operator)
   *    -> previous operator list ($inc, $push, $pushAll, $pull, $pullAll, $addUnique)
   *    -> added operator list    ($mul, $rename, $set, $unset, $min, $max, $currentDate)
   *    -> current operator list  ($inc, $mul, $rename, $set, $unset, $min, $max, $currentDate, $push, $pushAll, $pull, $pullAll, $addUnique)
   *
   */
  (function(Collection) {

    var _ = require('lodash'),
      debug = require('debug')('collection:extended'),
      dot = require('dot-object'),
      moment = require('moment'),
      date = require('date.js'),
      validation = require('validation');


    /**
     * Validate the request `body` against the `Collection` `properties`
     * and return an object containing any `errors`.
     *
     * @param {Object} body
     * @return {Object} errors
     */

    Collection.prototype.validate = function (body, create) {
      if(!this.properties) { this.properties = {}; }

      var keys = Object.keys(this.properties),
        props = this.properties,
        errors = {};

      keys.forEach(function (key) {
        var prop = props[key],
          val = body[key],
          type = prop.type || 'string';

        debug('validating %s against %j', key, prop);

        if(validation.exists(val)) {
          // coercion
          if(type === 'number') {
            val = Number(val);
          } else if (type === 'function' && typeof val !== 'function') {
            try {
              val = createFunction([{require: require}, val]);
            }
            catch (ex) {
              debug('Error from createFunction: %s\n  Stack: %s', ex, ex.stack);
            }

          } else if(type === 'date' && !(val instanceof Date)) {
            try {
              var temp = moment(val);
              if (!temp.isValid()) {
                temp = moment(date(val));
              }

              if (temp.isValid()) {
                val = new Date(temp);
              }
            }
            catch (ex) {}
          }

          if(!validation.isType(val, type)) {
            debug('failed to validate %s as %s', key, type);
            errors[key] = 'must be a ' + type;
          }
        } else if(prop.required) {
          debug('%s is required', key);
          if(create || body.hasOwnProperty(key)) {
            errors[key] = 'is required';
          }
        } else if(type === 'boolean') {
          body[key] = false;
        }
      });

      if(Object.keys(errors).length) { return errors; }
    };

    var _count = Collection.prototype.count;
    Collection.prototype.count = function(ctx, fn) {
      var origIsRoot = ctx.session.isRoot,
        restoreIsRootFn = function(err, result) {
          ctx.session.isRoot = origIsRoot;
          fn(err, result);
        };

      ctx.session.isRoot = true;

      _count.call(this, ctx, restoreIsRootFn);
    };

    var _indexOf = Collection.prototype.indexOf;
    Collection.prototype.indexOf = function(id, ctx, fn) {
      var origIsRoot = ctx.session.isRoot,
        restoreIsRootFn = function(err, result) {
          ctx.session.isRoot = origIsRoot;
          fn(err, result);
        };

      ctx.session.isRoot = true;

      _indexOf.call(this, id, ctx, restoreIsRootFn);
    };

    Collection.prototype.distinct = function(ctx, fn) {
      var collection = this,
        store = collection.store,
        sanitizedQuery = collection.sanitizeQuery(ctx.query || {});

      store.distinct(sanitizedQuery, function (err, result) {
        if (err) { return fn(err); }

        fn(null, {distinct: result});
      });
    };

    /**
     * Parse the `ctx.url` for an id
     *
     * @param {Context} ctx
     * @return {String} id
     */

    Collection.prototype.parseId = function(ctx) {
      if(ctx.url && ctx.url !== '/') {
        var id = ctx.url.split('/')[1];
        return (isIdentifier(id) || suggarIds.indexOf(id) !== -1) ? id : undefined;
      }
    };

    Collection.prototype.group = function(ctx, fn) {
      var collection = this,
        store = collection.store,
        sanitizedQuery = collection.sanitizeQuery(ctx.query || {});

      store.group(sanitizedQuery, function (err, result) {
        if (err) { return fn(err); }

        result = (result[0] && result[0].value) || {};
        fn(null, result);
      });
    };

    groupIds.slice(1).forEach(function(groupName) {
      Collection.prototype[groupName] = function(ctx, fn) {
        this.group(ctx, function(err, result){
          if (err) { return fn(err); }

          fn(null, _.pick(result, groupName));
        });
      };
    });


    Collection.prototype.handle = function (ctx) {
      // set id one wasnt provided in the query
      var id = ctx.query.id = ctx.query.id || this.parseId(ctx) || (ctx.body && ctx.body.id);

      if (ctx.req.method === 'GET' && id === 'index-of') {
        delete ctx.query.id;
        if (ctx.query.$fields) {
          id = ctx.query.$fields;
          delete ctx.query.$fields;
        } else {
          id = ctx.url.split('/').filter(function(p) { return p; })[1];
        }
        this.indexOf(id, ctx, ctx.done);
        return;
      }

      if (ctx.req.method === 'GET' && id === 'count') {
        delete ctx.query.id;
        this.count(ctx, ctx.done);
        return;
      }

      if (ctx.req.method === 'GET' && id === 'distinct') {
        delete ctx.query.id;
        this.distinct(ctx, ctx.done);
        return;
      }

      if (ctx.req.method === 'GET' && suggarIds.indexOf(id) !== -1) {
        delete ctx.query.id;
        this[id](ctx, ctx.done);
        return;
      }

      switch(ctx.req.method) {
      case 'GET':
        this.find(ctx, ctx.done);
        break;
      case 'PUT':
        if (typeof ctx.query.id !== 'string' && !ctx.req.isRoot) {
          ctx.done('must provide id to update an object');
          break;
        }
      /* falls through */
      case 'POST':
        this.save(ctx, ctx.done);
        break;
      case 'DELETE':
        this.remove(ctx, ctx.done);
        break;
      }
    };

    /**
     * Sanitize the request `body` against the `Collection` `properties`
     * and return an object containing only properties that exist in the
     * `Collection.config.properties` object.
     *
     * @param {Object} body
     * @return {Object} sanitized
     */

    function adaptValue(value, expected, actual) {
      if (!expected) {
        if (actual === 'string' && ['{', '['].indexOf(value[0]) !== -1) {
          return JSON.parse(value);
        }
        return value;
      }
      else if (expected === actual) {
        return value;
      }
      else if (expected === 'array' && Array.isArray(value)) {
        return value;
      }
      else if (expected === 'array' && actual === 'string' && value[0] === '[') {
        return JSON.parse(value);
      }
      else if (expected === 'object' && actual === 'string' && value[0] === '{') {
        return JSON.parse(value);
      }
      else if (expected === 'number' && actual === 'string') {
        return parseFloat(value);
      }
      else if (expected === 'string' && actual === 'number') {
        return '' + value;
      }
      else if (expected === 'boolean' && actual === 'string') {
        return  (['1', 'y', 'true'].indexOf(value) !== -1) ? true : false;
      }
      else if (typeof value !== 'undefined') {
        if (expected === 'boolean') {
          return !!value;
        }
        else if(value === null && (expected === 'string' || expected === 'array')) {
          // keep null
          return value;
        }

      }
      return value;
    }

    Collection.prototype.sanitize = function (body) {
      if(!this.properties) { return {}; }

      var copy = {},
        sanitized = {},
        props = this.properties,
        propskeys = Object.keys(props),
        bodyKeys = Object.keys(body),
        subTypes = body._subTypes || {};

      // copy body obj
      Object.keys(body).forEach(function (key) {
        copy[key] = _.clone(body[key]);
      });

      dot.object(body);

      propskeys.forEach(function (key) {
        var prop = props[key],
          expected = prop.type,
          val = body[key],
          actual = typeof val,
          matchingKeyReg = new RegExp('^' + key + '\\.'),
          matchingKeys = bodyKeys.filter(function(k){ return matchingKeyReg.test(k); });

        // skip properties that do not exist
        if(!prop) { return; }

        if(expected === actual) {
          if (actual === 'object' && matchingKeys.length) {
            matchingKeys.forEach(function(k){
              sanitized[k] = adaptValue(copy[k], subTypes[k], typeof copy[k]);
            });
          } else {
            sanitized[key] = val;
          }
        } else {
          sanitized[key] = adaptValue(val, expected, actual);
        }
      });

      return sanitized;
    };

    Collection.prototype.sanitizeQuery = function (query) {
      var sanitized = {},
        props = this.properties || {},
        keys = (query && Object.keys(query)) || [],
        subTypes = (query && query._subTypes) || {};

      keys.forEach(function (key) {

        // skip properties that have already been imported
        if(key in sanitized) { return; }

        var
          propKey = key.split('.')[0],
          prop = props[propKey],
          expected = prop && prop.type,
          val = query[key],
          actual = (propKey === key) ? typeof val : 'object',
          matchingKeyReg = new RegExp('^' + propKey + '\\.'),
          matchingKeys = keys.filter(function(k){ return matchingKeyReg.test(k); });

        // skip properties that do not exist, but allow $ queries and id
        if(!prop && key.indexOf('$') !== 0 && key !== 'id') { return; }

        // hack - $limitRecursion and $skipEvents are not mongo properties so we'll get rid of them, too
        if (key === '$limitRecursion') { return; }
        if (key === '$skipEvents') { return; }

        if(actual === expected && expected === 'object') {
          if (matchingKeys.length) {
            matchingKeys.forEach(function(k){
              sanitized[k] = adaptValue(query[k], subTypes[k], typeof query[k]);
            });
          } else {
            sanitized[key] = val;
          }
        }
        else if (typeof val !== 'undefined') {
          sanitized[key] = adaptValue(val, expected, actual);
        }
      });
      debug('sanitizeQuery -> query: %j\n ---------------> sanitized: %j', query, sanitized);
      return sanitized;
    };

    Collection.prototype.execCommands = function (type, obj, commands, previous) {
      // var storeCommands = null;
      debug('execCommands -> -------------- type: %s ----------------', type);
      debug('execCommands -> obj:%j\n', obj);
      debug('execCommands -> commands:%j\n', commands);

      try {
        if(type === 'insert') {
          Object.keys(commands).forEach(function (key) {
            if(typeof commands[key] === 'object') {
              Object.keys(commands[key]).forEach(function (k) {
                if(k[0] !== '$') { return; }

                var val = commands[key][k];

                if(k === '$setOnInsert') { /* added */
                  obj[key] = val;
                }

              });
            }
          });
        }

        if(type === 'update') {
          debug('execCommands -> previous:%j', previous);
          Object.keys(commands).forEach(function (key) {
            if(typeof commands[key] === 'object') {
              Object.keys(commands[key]).forEach(function (k) {
                if(k[0] !== '$') { return; }

                var val = commands[key][k];
                var prev = dot.pick(key, previous);

                debug('\t -------------------------------------------------------');
                debug('\t ---  val:%s = commands[key:%s][k:%s]', val, key, k);
                debug('\t --- prev:%s = dot.pick(key:%s, previous)', prev, key);
                debug('\t -------------------------------------------------------');

                if(k === '$inc') {
                  debug('\t $inc --- before setting obj[key:%s]', key);
                  if(!prev) { prev = 0; }
                  prev = parseFloat(prev);
                  val = prev + parseFloat(val);
                  dot.set(key, val, obj, true);
                }
                if(k === '$mul') { /* added */
                  debug('\t $mul --- before setting obj[key:%s]', key);
                  if(!prev) { prev = 0; }
                  prev = parseFloat(prev);
                  val = prev * parseFloat(val);
                  dot.set(key, val, obj, true);
                }
                if(k === '$rename') { /* added */
                  debug('\t $rename --- before setting obj[key:%s]', key);
                  // we need to send $unset and $rename to the store
                  dot.set(val, prev, obj, true);
                  dot.del(key, obj);
                  // storeCommands = storeCommands || {};
                  // storeCommands.$unset = storeCommands.$unset || {};
                  // storeCommands.$unset[key] = true;
                }
                if(k === '$set') { /* added */
                  debug('\t $set --- before setting obj[key:%s] to val:%s', key, val);
                  dot.set(key, val, obj, true);
                  debug('\t $set --- after setting obj[key:%s] to val:%s --> %s', key, val, dot.pick(key, obj));
                }
                if(k === '$unset') { /* added */
                  debug('\t $unset --- before setting obj[key:%s]', key);
                  dot.del(key, obj);
                  // storeCommands = storeCommands || {};
                  // storeCommands.$unset = storeCommands.$unset || {};
                  // storeCommands.$unset[key] = true;
                }
                if(k === '$min') { /* added */
                  debug('\t $min --- before setting obj[key:%s]', key);
                  if(!prev) { prev = 0; }
                  prev = parseFloat(prev);
                  if (prev > val) {
                    dot.set(key, val, obj, true);
                  }
                }
                if(k === '$max') { /* added */
                  debug('\t $max --- before setting obj[key:%s]', key);
                  if(!prev) { prev = 0; }
                  prev = parseFloat(prev);
                  if (prev < val) {
                    dot.set(key, val, obj, true);
                  }
                }
                if(k === '$currentDate') { /* added */
                  debug('\t $currentDate --- before setting obj[key:%s]', key);
                  dot.str(key, new Date().getTime(), obj);
                  dot.set(key, new Date().getTime(), obj, true);
                }
                if(k === '$push') {
                  debug('\t $push --- before setting obj[key:%s]', key);
                  if(Array.isArray(prev)) {
                    val = [].concat(prev).push(val);
                    dot.set(key, val, obj, true);
                  } else {
                    dot.set(key, [val], obj, true);
                  }
                }
                if(k === '$pushAll') {
                  debug('\t $pushAll --- before setting obj[key:%s]', key);
                  if(Array.isArray(prev)) {
                    val = [].concat(prev).concat(val);
                  }
                  dot.set(key, val, obj, true);
                }
                if (k === '$pull') {
                  debug('\t $pull --- before setting obj[key:%s]', key);
                  if(Array.isArray(prev)) {
                    val = prev.filter(function(item) {
                      return item !== val;
                    });
                    dot.set(key, val, obj, true);
                  }
                }
                if (k === '$pullAll') {
                  debug('\t $pullAll --- before setting obj[key:%s]', key);
                  if(Array.isArray(prev)) {
                    if(Array.isArray(val)) {
                      val = prev.filter(function(item) {
                        return val.indexOf(item) === -1;
                      });
                      dot.set(key, val, obj, true);
                    }
                  }
                }
                if (k === '$addUnique') {
                  debug('\t $addUnique --- before setting obj[key:%s]', key);
                  val = Array.isArray(val) ? val : [val];
                  if(Array.isArray(prev)) {
                    val = _.union(prev, val);
                  }
                  dot.set(key, val, obj, true);
                }
              });
            } else {
              debug('############ typeof commands[key:%s] is %s, very bad !', key, typeof commands[key]);
            }
          });
        }
      } catch(e) {
        debug('error while executing commands', type, obj, commands);
        debug('error details: %s\nerror stack: %s', e, e.stack);
      }
      return this;
    };

    /**
     * Find all the objects in a collection that match the given
     * query. Then execute its get script using each object.
     *
     * @param {Context} ctx
     * @param {Function} fn(err, result)
     */

    Collection.prototype.find = function (ctx, fn) {
      var collection = this,
        store = this.store,
        query = ctx.query || {},
        // session = ctx.session,
        // client = ctx.dpd,
        // errors,
        data,
        sanitizedQuery;

      function done(err, result) {
        if (ctx.res.internal) {
          ctx.res.setHeader('Accept-Range', collection.name + ' ' + 90);
        }

        debug('Get listener called back with', err || (Array.isArray(result) ? result.slice(0, 3) : result));

        if(typeof query.id === 'string' && (result && result.length === 0) || !result) {
          err = err || {
            message: 'not found',
            statusCode: 404
          };
          debug('could not find object by id %s', query.id);
        }

        if(err) { return fn(err); }

        if(typeof query.id === 'string' && Array.isArray(result)) {
          return fn(null, result[0]);
        }

        if (Array.isArray(result) && typeof(result.totalCount) !== 'undefined') {
          var start = 0,
            end = result.length ? result.length - 1 : 0;

          if (result.length) {
            if (ctx.res.internal && result.length < result.totalCount) {
              ctx.res.statusCode = 206;
            }
            if (query.$skip) {
              start += query.$skip;
              end += query.$skip;
            }
          }

          if (ctx.res.internal) {
            ctx.res.setHeader('X-Total-Count', result.totalCount);
            ctx.res.setHeader('Content-Range', start + '-' + end + '/' + result.totalCount);
          }
          // TODO: implement Link header
          // http://blog.octo.com/designer-une-api-rest/
          delete result.totalCount;
        }
        fn(null, result);
      }

      function doFind() {
        // resanitize query in case it was modified from BeforeRequest event
        sanitizedQuery = collection.sanitizeQuery(query);

        sanitizedQuery.$limit = sanitizedQuery.$limit || 90;
        if (sanitizedQuery.$limit > 90) {
          var err = {
            message: 'invalid range',
            statusCode: 400
          };
          return done(err);
        }

        debug('finding %j; sanitized %j', query, sanitizedQuery);
        store.find(sanitizedQuery, function (err, result) {
          debug('Find Callback');
          if(err) { return done(err); }
          debug('found %s', err || (result && (Array.isArray(result) ? result.length + ' ' + collection.name : result)) || ('no matching ' + collection.name) );
          if(!collection.shouldRunEvent(collection.events.Get, ctx)) {
            return done(err, result);
          }

          var errors = {};

          if(Array.isArray(result)) {

            var remaining = result && result.length;
            var totalCount = result && result.totalCount;
            if(!remaining) { return done(err, result); }
            result.forEach(function (data) {
              // domain for onGet event scripts
              var domain = collection.createDomain(data, errors);

              collection.events.Get.run(ctx, domain, function (err) {
                if (err) {
                  if (err instanceof Error) {
                    return done(err);
                  } else {
                    errors[data.id] = err;
                  }
                }

                remaining--;
                if(!remaining) {
                  result = result.filter(function(r) {
                    return !errors[r.id];
                  });
                  result.totalCount = totalCount;
                  done(null, result);
                }
              });
            });
          } else {
            // domain for onGet event scripts
            data = result;
            var domain = collection.createDomain(data, errors);

            collection.events.Get.run(ctx, domain, function (err) {
              if(err) { return done(err); }

              done(null, data);
            });
          }
        });
      }

      var beforeRequestDomain = { event: 'GET' };
      collection.addDomainAdditions(beforeRequestDomain);
      collection.doBeforeRequestEvent(ctx, beforeRequestDomain, function(err) {
        if (err) { return fn(err); }
        doFind();
      });
    };

    /**
     * Execute the onPost or onPut listener. If it succeeds,
     * save the given item in the collection.
     *
     * @param {Context} ctx
     * @param {Function} fn(err, result)
     */

    Collection.prototype.save = function (ctx, fn) {
      var collection = this,
        store = this.store,
        // session = ctx.session,
        item = ctx.body,

        query = ctx.query || {},
        // client = ctx.dpd,
        errors = {};

      function done(err, item) {
        errors = domain && domain.hasErrors() && {errors: errors};
        debug('errors: %j', err);
        fn(errors || err, item);
      }

      if(!item) {
        return done('You must include an object when saving or updating.');
      }

      // extract mongoCommands first and transform them to commands
      var commands = {};
      Object.keys(item).forEach(function (key) {
        if(key[0] === '$' && item[key] && typeof item[key] === 'object') {
          Object.keys(item[key]).forEach(function (k) {
            if (!item[k]) { item[k] = {}; }
            item[k][key] = item[key][k];
          });
        }
      });

      // build command object
      commands = {};
      Object.keys(item).forEach(function (key) {
        if(item[key] && typeof item[key] === 'object' && !Array.isArray(item[key])) {
          Object.keys(item[key]).forEach(function (k) {
            if(k[0] === '$') {
              commands[key] = item[key];
            }
          });
        }
      });

      item = this.sanitize(item);

      // handle id on either body or query
      if(item.id) {
        query.id = item.id;
      }

      debug('saving %j with id %s', item, query.id);

      var domain = collection.createDomain(item, errors);

      domain.protectedKeys = [];

      domain.protect = function(property) {
        var propParts = property.split('.'),
          realProp = propParts.pop(),
          data = propParts.length ? dot.pick(propParts.join('.'), domain.data) : domain.data;

        if (data.hasOwnProperty(realProp)) {
          domain.protectedKeys.push(property);
          delete data[realProp];
        }
      };

      domain.changed =  function (property) {
        var propParts = property.split('.'),
          realProp = propParts.pop(),
          data = propParts.length ? dot.pick(propParts.join('.'), domain.data) || {} : domain.data;

        if(data.hasOwnProperty(realProp)) {
          if(domain.previous && _.isEqual(dot.pick(property, domain.previous), data[realProp])) {
            return false;
          }

          return true;
        }
        return false;
      };

      domain.previous = {};

      function put() {
        var id = query.id,
          sanitizedQuery = collection.sanitizeQuery(query),
          prev = {},
          keys;

        store.first(sanitizedQuery, function(err, obj) {
          if(!obj) {
            if (Object.keys(sanitizedQuery) === 1) {
              return done(new Error('No object exists with that id'));
            } else {
              return done(new Error('No object exists that matches that query'));
            }
          }
          if(err) { return done(err); }

          // copy previous obj
          Object.keys(obj).forEach(function (key) {
            prev[key] = _.clone(obj[key], true);
          });

          // merge changes ( obj[key] = item[key] )
          keys = Object.keys(item);
          dot.object(item);
          keys.forEach(function (key) {
            dot.copy(key, key, item, obj);
          });

          prev.id = id;
          item = obj;
          domain['this'] = item;
          domain.data = item;
          domain.previous = prev;

          // var storeCommands = collection.execCommands('update', item, commands, prev) || {};
          collection.execCommands('update', item, commands, prev);

          var errs = collection.validate(item);

          if (errs) { return done({errors: errs}); }

          // if (storeCommands) {
          //   _.assign(item, storeCommands);
          // }

          function runPutEvent(err) {
            if(err) {
              return done(err);
            }

            if(collection.shouldRunEvent(collection.events.Put, ctx)) {
              collection.events.Put.run(ctx, domain, commit);
            } else {
              commit();
            }
          }

          function commit(err) {
            if(err || domain.hasErrors()) {
              return done(err || errors);
            }

            delete item.id;
            store.update({id: query.id}, item, function (err) {
              if(err) { return done(err); }
              item.id = id;
              collection.doAfterCommitEvent('PUT', ctx, item, prev, domain.protectedKeys);
              done(null, item);
            });
          }

          if (collection.shouldRunEvent(collection.events.Validate, ctx)) {
            collection.events.Validate.run(ctx, domain, function (err) {
              if(err || domain.hasErrors()) { return done(err || errors); }
              runPutEvent(err);
            });
          } else {
            runPutEvent();
          }
        });
      }

      function post() {
        collection.execCommands('insert', item, commands);
        var errs = collection.validate(item, true);

        if (errs) { return done({errors: errs}); }

        // generate id before event listener
        item.id = store.createUniqueIdentifier();

        function commit(){
          store.insert(item, function(err, data) {
            if (err) { return done(err); }
            collection.doAfterCommitEvent('POST', ctx, item);
            done(null, data);
          });
        }

        if(collection.shouldRunEvent(collection.events.Post, ctx)) {
          collection.events.Post.run(ctx, domain, function (err) {
            if(err) {
              debug('onPost script error %j', err);
              return done(err);
            }
            if(err || domain.hasErrors()) { return done(err || errors); }
            debug('inserting item', item);

            commit();
          });
        } else {
          commit();
        }
      }

      var beforeRequestDomain = { event: 'POST', data: item };
      collection.addDomainAdditions(beforeRequestDomain);

      if (query.id) {
        beforeRequestDomain.event = 'PUT';
        collection.doBeforeRequestEvent(ctx, beforeRequestDomain, function(err) {
          if (err) { return fn(err); }
          put();
        });
      }
      else if (collection.shouldRunEvent(collection.events.Validate, ctx)) {
        collection.doBeforeRequestEvent(ctx, beforeRequestDomain, function(err) {
          if (err) { return fn(err); }
          collection.events.Validate.run(ctx, domain, function (err) {
            if(err || domain.hasErrors()) { return done(err || errors); }
            post();
          });
        });
      } else {
        collection.doBeforeRequestEvent(ctx, beforeRequestDomain, function(err) {
          if (err) { return fn(err); }
          post();
        });
      }
    };

  })(require('deployd/lib/resources/collection'));
}

function extendStore() {
  'use strict';
  /*
   * Extensions for 'deployd/lib/db'.Store
   *
   * override Store.prototype.update(query, object, fn) for debugging purpose
   * add Store.prototype.distinct( query, fn) to respond to Collection.prototype.distinct
   * add Store.prototype.min(      query, fn) to respond to Collection.prototype.min
   * add Store.prototype.max(      query, fn) to respond to Collection.prototype.max
   *
   */
  (function(Store){

    var _ = require('lodash'),
      mongo = require('mongodb'),
      Code = mongo.Code,
      debug = require('debug')('db:extended');

    function stripFields(query) {
      if(!query) { return; }
      var fields = query.$fields;
      if(fields) { delete query.$fields; }
      return fields;
    }

    function stripOptions(query) {
      var options = {};
      if(!query) { return options; }
      // performance
      if(query.$limit) { options.limit = query.$limit; }
      if(query.$skip) { options.skip = query.$skip; }
      if(query.$sort || query.$orderby) { options.sort = query.$sort || query.$orderby; }
      delete query.$limit;
      delete query.$skip;
      delete query.$sort;
      return options;
    }

    /**
     * Find the distinct values of a given field for objects in the store that match the given query.
     *
     * Example:
     *
     *     db
     *       .connect({host: 'localhost', port: 27015, name: 'test'})
     *       .createStore('testing-store')
     *       .distinct('name', {foo: 'bar'}, fn)
     *
     * @param {Object} query
     * @param {Function} callback(err, valuesArray)
     */

    Store.prototype.distinct = function(query, fn) {
      var store = this;
      if (typeof query === 'function') {
        fn = query;
        query = {};
      }
      else if (query) {
        this.scrubQuery(query);
      }

      var fields = stripFields(query),
        options = stripOptions(query),
        field = typeof fields === 'object' ? Object.keys(fields)[0] : fields;

      store.getCollection(function (err, col) {
        if (err) { return fn(err); }
        col.distinct(field, query, options, function(err, values) {
          if (err) { return fn(err); }
          fn(null, values);
        });
      });
    };

    /* function.js group functions are from
     *  - https://gist.github.com/RedBeard0531/1886960
     *  - https://gist.github.com/Pyrolistical/8139958
     *
     *   > load('functions.js')
     *   > db.stuff.drop()
     *   false
     *   > db.stuff.insert({value:1})
     *   > db.stuff.insert({value:2})
     *   > db.stuff.insert({value:2})
     *   > db.stuff.insert({value:2})
     *   > db.stuff.insert({value:3})
     *   > db.stuff.mapReduce(map, reduce, {finalize:finalize, out:{inline:1}}).results[0]
     *   {
     *     "_id" : 1,
     *     "value" : {
     *       "sum" : 10,
     *       "min" : 1,
     *       "max" : 3,
     *       "count" : 5,
     *       "diff" : 2,
     *       "avg" : 2,
     *       "variance" : 0.4,
     *       "stddev" : 0.6324555320336759
     *     }
    }
     */
    function groupMap() {
      /* global emit, pickValue, fieldName */
      /* jshint validthis:true */

      var value = pickValue(this, fieldName);
      emit(1, {
        sum: !value ? 0 : parseFloat(value), // the field you want stats for
        min: (value === null || value === undefined) ? Infinity : parseFloat(value),
        max: (value === null || value === undefined) ? -Infinity : parseFloat(value),
        count: 1,
        diff: 0
      });
    }

    function groupReduce(key, values) {
      return values.reduce(function reduce(previous, current/*, index, array*/) {
        var delta = previous.sum/previous.count - current.sum/current.count;
        var weight = (previous.count * current.count)/(previous.count + current.count);

        return {
          sum: previous.sum + current.sum,
          min: Math.min(previous.min, current.min),
          max: Math.max(previous.max, current.max),
          count: previous.count + current.count,
          diff: previous.diff + current.diff + delta*delta*weight
        };
      });
    }

    function groupFinalize(key, value) {
      value.avg = value.sum / value.count;
      value.population_variance = value.diff / value.count;
      value.population_standard_deviation = Math.sqrt(value.population_variance);
      value.sample_variance = value.diff / (value.count - 1);
      value.sample_standard_deviation = Math.sqrt(value.sample_variance);
      delete value.diff;
      return value;
    }

    /*
     * mapReduce syntax: collection.mapReduce(map, reduce, options, callback)
     * => returns a promise if no callback is provided
     *
     * map: map Function
     * reduce: reduce Function
     * options: options objects                    (optionnal settings)
     * callback: callback function(err, res, ...)  (optionnal settings)
     *
     * Available options for mapReduce
     *  NAME                        TYPE        DEFAULT     Description and/or available options
     * ----------------------------------------------------------------------------------------------------------------------
     * - readPreference           | string    | null   | (ReadPreference.PRIMARY, ReadPreference.PRIMARY_PREFERRED,
     *                            |           |        |  ReadPreference.SECONDARY, ReadPreference.SECONDARY_PREFERRED,
     *                            |           |        |  ReadPreference.NEAREST)
     * ----------------------------------------------------------------------------------------------------------------------
     * - out                      | object    | null   | ({inline:1}, {replace:'collectionName'},
     *                            |           |        | {merge:'collectionName'}, {reduce:'collectionName'})
     * ----------------------------------------------------------------------------------------------------------------------
     * - query                    | object    | null   | Query filter object.
     * ----------------------------------------------------------------------------------------------------------------------
     * - sort                     | object    | null   | Sorts the input objects using this key. Useful for optimization, 
     *                            |           |        | like sorting by the emit key for fewer reduces.
     * ----------------------------------------------------------------------------------------------------------------------
     * - limit                    | number    | null   | Number of objects to return from collection.
     * ----------------------------------------------------------------------------------------------------------------------
     * - keeptemp                 | boolean   | false  | Keep temporary data.
     * ----------------------------------------------------------------------------------------------------------------------
     * - finalize                 | function  | null   | Finalize function.
     *                            | or string |        |
     * ----------------------------------------------------------------------------------------------------------------------
     * - scope                    | object    | null   | Can pass in variables that can be access from map/reduce/finalize.
     * ----------------------------------------------------------------------------------------------------------------------
     * - jsMode                   | boolean   | false  | It is possible to make the execution stay in JS.
     *                            |           |        | Provided in MongoDB > 2.0.X.
     * ----------------------------------------------------------------------------------------------------------------------
     * - verbose                  | boolean   | false  | Provide statistics on job execution time.
     * ----------------------------------------------------------------------------------------------------------------------
     * - bypassDocumentValidation | boolean   | false  | Allow driver to bypass schema validation in MongoDB 3.2 or higher.
     * ----------------------------------------------------------------------------------------------------------------------
     *
     * collection.mapReduce(mapFn, reduceFn, {finalize: finalizeFn, out : {inline: 1}, verbose:true}, function(err, results, stats) {})
     */
    Store.prototype.group = function(query, fn) {

      var store = this;
      if (typeof query === 'function') {
        fn = query;
        query = {};
      }
      else if (query) {
        this.scrubQuery(query);
      }

      var fields = stripFields(query),
        options = stripOptions(query),
        field = typeof fields === 'object' ? Object.keys(fields)[0] : fields,
        pickValue = function(row, fieldName) {
            var parts = fieldName.split('.'),
              name = parts.pop(),
              empty = {};
            if (parts.length) {
              parts.forEach(function(part){
                row = (typeof row[part] === 'object') ? row[part] : empty;
              });
            }
            return !row[name] ? null : parseFloat(row[name]);
          },
        groupOptions = {
          finalize: groupFinalize,
          out:      { inline:1 },
          query: query,
          sort: options.sort || null,
          limit: options.limit || null,
          scope: {
            pickValue: new Code(pickValue.toString()),
            fieldName: field
          }
        };

      store.getCollection(function (err, col) {
        if (err) { return fn(err); }
        col.mapReduce(groupMap, groupReduce, groupOptions, function(err, results/*, stats*/) {
          if (err) { return fn(err); }
          fn(null, results);
        });
      });

    };


    /**
     * Find all objects in the store that match the given query.
     *
     * Example:
     *
     *     db
     *       .connect({host: 'localhost', port: 27015, name: 'test'})
     *       .createStore('testing-store')
     *       .find({foo: 'bar'}, fn)
     *
     * @param {Object} query
     * @param {Function} callback(err, obj)
     */

    Store.prototype.find = function (query, fn) {
      var store = this;
      if(typeof query === 'function') {
        fn = query;
        query = {};
      }
      else if (query){
        this.scrubQuery(query);
      }

      // fields
      var fields = stripFields(query),
        options = stripOptions(query); // limit, skip and sort

      if (!_.isObject(fields)) { fields = undefined; }

      this.getCollection(function (err, col) {
        if (err) {
          fn(err);
          return;
        }
        if(query._id) {
          if(fields) {
            col.findOne(query, fields, options, function (err, obj) {
              if (err) {
                fn(err);
                return;
              }
              store.identify(query);
              fn(err, store.identify(obj));
            });
          } else {
            col.findOne(query, options, function (err, obj) {
              if (err) {
                fn(err);
                return;
              }
              store.identify(query);
              fn(err, store.identify(obj));
            });
          }
        } else {
          var cursor;
          if(fields) {
            cursor = col.find(query, fields);
          } else {
            cursor = col.find(query);
          }
          if (typeof options.sort === 'object') {
            if (Array.isArray(options.sort)) {
              if (Array.isArray(options.sort[0])) {
                cursor = cursor.sort(_.zipObject(options.sort));
              } else {
                cursor = cursor.sort(_.zipObject([options.sort]));
              }
            } else {
              cursor = cursor.sort(options.sort);
            }
          }
          cursor.count(function(err, totalCount){
            if (err) {
              fn(err);
              return;
            }

            if (options.skip) {
              cursor = cursor.skip(options.skip);
            }

            if (options.limit) {
              cursor = cursor.limit(options.limit);
            }

            cursor.toArray(function (err, arr) {
              if (err) {
                fn(err);
                return;
              }
              arr = store.identify(arr);
              arr.totalCount = totalCount;
              fn(err, arr);
            });
          });
        }

      });
    };

    /**
     * Update an object or objects in the store that match the given query.
     *
     * Example:
     *
     *     db
     *       .connect({host: 'localhost', port: 27015, name: 'test'})
     *       .createStore('testing-store')
     *       .update({id: '<an object id>'}, fn)
     *
     * @param {Object} query
     * @param {Object} object
     * @param {Function} callback(err, obj)
     */

    Store.prototype.update = function (query, object, fn) {
      var store = this,
        multi = false,
        command = {};

      if(typeof query === 'string') { query = {id: query}; }
      if(typeof query !== 'object') { throw new Error('update requires a query object or string id'); }
      if(query.id) {
        this.scrubQuery(query);
      }  else {
        multi = true;
      }

      debug('update - 1 -   query %j', query);
      debug('update - 1 -  object %j', object);

      stripFields(query);

      //Move $ queries outside of the $set command
      Object.keys(object).forEach(function(k) {
        if (k.indexOf('$') === 0) {
          command[k] = object[k];
          delete object[k];
        }
      });

      debug('update - 2 -   query %j', query);
      debug('update - 2 -  object %j', object);
      debug('update - 2 - command %j', command);

      if(Object.keys(object).length) {
        command.$set = object;
      }

      multi = query._id ? false : true;

      debug('update - 3 - command %j', command);

      store.getCollection(function (err, col) {
        if (err) {
          fn(err);
          return;
        }
        col.update(query, command, { multi: multi }, function (err, r) {
          if (err) {
            fn(err);
            return;
          }
          store.identify(query);
          fn(err, r ? { count: r.result.n } : null);
        }, multi);
      });
    };

  })(require('deployd/lib/db').Store);
}

extension.extend = function() {
  'use strict';

  extendConfig();
  extendScript();
  extendCollection();
  extendStore();
};

module.exports = extension;
