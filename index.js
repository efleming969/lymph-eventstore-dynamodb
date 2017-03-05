var AWS = require( "aws-sdk" )

var EventStore = function( config ) {
  this.tableName = config.tableName

  this.db = new AWS.DynamoDB.DocumentClient( {
    region: config.region
  , endpoint: config.endpoint
  } )
}

EventStore.prototype.get = function( id, version, callback ) {
  var db = this.db
  var tableName = this.tableName

  var params = {
    TableName: tableName
  , KeyConditionExpression: "id = :id and version >= :version"
  , ExpressionAttributeValues: { ":id": id, ":version": version }
  }

  db.query( params, function( err, data ) {
    if ( err )
      callback( console.log( err ) )
    else
      callback( data.Items.map( function( item ) {
        return Object.assign( {}, item, { data: JSON.parse( item.data ) } )
      } ) )
  } )
}

EventStore.prototype.append = function( aggregate, events, callback ) {
  var db = this.db
  var tableName = this.tableName

  var enrichedEvents = events.map( function( event, index ) {
    return {
      id: aggregate.id
    , version: aggregate.version + index
    , created: new Date().getTime()
    , name: event.name
    , data: JSON.stringify( event.data )
    }
  } )

  var putCount = events.length

  enrichedEvents.forEach( function( enrichedEvent ) {
    var params = {
      TableName: tableName
    , Item: enrichedEvent
    }

    db.put( params, function( err ) {
      if ( err )
        console.log( err )

      if ( putCount == 1 )
        callback( enrichedEvents )

      putCount = putCount - 1
    } )
  } )
}

exports.create = function( config ) {
  return new EventStore( config )
}
