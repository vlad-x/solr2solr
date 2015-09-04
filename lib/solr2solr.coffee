path = require 'path'
solr = require 'solr-client'
_ =    require 'underscore'
querystring =    require 'querystring'

class SolrToSolr

  go: (@config) ->
    @sourceClient = solr.createClient(@config.from)
    @destClient   = solr.createClient(@config.to)

    @sourceClient.query = @destClient.query = (query, queryOptions, callback) ->
      if ('rows' in queryOptions && !queryOptions.rows)
        delete queryOptions.rows; # ugly fix

      queryOptions.q = query;
      this.search(querystring.encode(queryOptions), callback);

    if @config.from.user
      console.log @config.from
      @sourceClient.basicAuth(@config.from.user, @config.from.password);

    if @config.to.user
      @destClient.basicAuth(@config.to.user, @config.to.password);

    @nextBatch(@config.start)

  nextBatch: (start) ->
    console.log "Querying starting at #{start}"
    @sourceClient.query @config.query, {rows:@config.rows, start:start}, (err, response) =>
      return console.log "Some kind of solr query error #{err}" if err?
      # console.log 'response', response
      # responseObj = JSON.parse response
      responseObj = response

      newDocs = @prepareDocuments(responseObj.response.docs, start)
      @writeDocuments newDocs, =>
        start += @config.rows
        if responseObj.response.numFound > start
          @nextBatch(start)
        else
          @destClient.commit()

  prepareDocuments: (docs, start) =>
    for doc in docs
      newDoc = {}
      if @config.clone
        for cloneField of doc
          newDoc[cloneField] = doc[cloneField]
      else
        for copyField in @config.copy
          newDoc[copyField] = doc[copyField] if doc[copyField]?
      for transform in @config.transform
        newDoc[transform.destination] = doc[transform.source] if doc[transform.source]?
      for fab in @config.fabricate
        vals = fab.fabricate(newDoc, start)
        newDoc[fab.name] = vals if vals?
      start++
      delete newDoc._version_
      newDoc

  writeDocuments: (documents, done) ->
    docs = []
    docs.push documents
    if @config.duplicate.enabled
      for doc in documents
        for num in [0..@config.duplicate.numberOfTimes]
          newDoc = _.extend({}, doc)
          newDoc[@config.duplicate.idField] = "#{doc[@config.duplicate.idField]}-#{num}"
          docs.push newDoc

    @destClient.add _.flatten(docs), (err) =>
      console.log err if err
      @destClient.commit()
      done()

exports.go = (config) ->
  (new SolrToSolr()).go(config)
