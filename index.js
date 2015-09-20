
(function(Script) {
  'use strict';

  var _run = Script.prototype.run;
  Script.prototype.run = function(ctx, domain, fn) {
    if (typeof domain === 'object') {
      domain.process = function() {
        return process;
      };
      domain.require = function(resource) {
        return require(resource);
      };
      domain.context = function() { // access Context via context()
        return ctx;
      };
    }
    _run.call(this, ctx, domain, fn);
  };
})(require('deployd/lib/script'));

(function(Collection) {
  'use strict';

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

  Collection.prototype.handle = function (ctx) {
    // set id one wasnt provided in the query
    ctx.query.id = ctx.query.id || this.parseId(ctx) || (ctx.body && ctx.body.id);

    if (ctx.req.method === 'GET' && ctx.query.id === 'count') {
      delete ctx.query.id;
      this.count(ctx, ctx.done);
      return;
    }

    if (ctx.req.method === 'GET' && ctx.query.id === 'distinct') {
      delete ctx.query.id;
      this.distinct(ctx, ctx.done);
      return;
    }

    if (ctx.req.method === 'GET' && ctx.query.id === 'index-of') {
      delete ctx.query.id;
      var id = ctx.url.split('/').filter(function(p) { return p; })[1];
      this.indexOf(id, ctx, ctx.done);
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

})(require('deployd/lib/resources/collection'));

(function(Store){
  'use strict';

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

})(require('deployd/lib/db').Store);
