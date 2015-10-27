path = require 'path'
solr = require 'solr-client'
_ =    require 'underscore'
querystring =    require 'querystring'

class SolrToSolr

  go: (@config) ->
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'
    @sourceClient = solr.createClient(@config.from)
    @destClient   = solr.createClient(@config.to)
    @numProcessed = 0

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

    if @config.cursorMark
      nextBatchData = {cursorMark: @config.cursorMark}
    else
      nextBatchData = {start: @config.start}
    @nextBatch(nextBatchData)

  nextBatch: (nextBatchData) ->
    queryOptions = {rows:@config.rows, sort:@config.sort || 'score desc'}
    if nextBatchData.cursorMark
      console.log "Query using cursorMark #{nextBatchData.cursorMark}"
      queryOptions.cursorMark = nextBatchData.cursorMark
      queryOptions.sort = queryOptions.sort + ', id desc' # adding unique field to sort because it's necessary when using cursorMark
    else
      console.log "Query starting at #{nextBatchData.start}"
      queryOptions.start = nextBatchData.start

    @sourceClient.query @config.query, queryOptions, (err, response) =>
      return console.log "Source Solr query error #{err}" if err?
      # console.log 'response', response
      # responseObj = JSON.parse response
      responseObj = response

      newDocs = @prepareDocuments(responseObj.response.docs, nextBatchData.start || 0)
      @writeDocuments newDocs, =>
        @numProcessed += @config.rows
        console.log "Done #{@numProcessed} rows"
        if nextBatchData.cursorMark
          if responseObj.nextCursorMark == nextBatchData.cursorMark # cursorMark same as previous means we reached the end
            @destClient.commit()
          else
            nextBatchData.cursorMark = responseObj.nextCursorMark
            @nextBatch(nextBatchData)
        else
          nextBatchData.start += @config.rows
          if responseObj.response.numFound > nextBatchData.start
            @nextBatch(nextBatchData)
          else
            @destClient.commit()

  prepareDocuments: (docs, start) =>
    for doc in docs
      newDoc = {}
      if @config.process
        doc = @config.process doc
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
