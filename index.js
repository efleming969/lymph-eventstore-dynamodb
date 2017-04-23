var AWS = require( "aws-sdk" )

var deserializeData = function( item ) {
  return Object.assign( {}, item, { data: JSON.parse( item.data ) } )
}

var serializeData = function( item ) {
  return Object.assign(
    {}, item, { data: JSON.stringify( item.data ) } )
}

var EventStore = function( tableName, endpoint ) {
  this.tableName = tableName
  console.log( "connecting to", endpoint )
  this.db = new AWS.DynamoDB.DocumentClient( { endpoint: endpoint } )
}

EventStore.prototype.getAll = function( callback ) {
  var db = this.db
  var tableName = this.tableName
  var params = { TableName: tableName }

  db.scan( params, function( err, data ) {
    callback( err ?  console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

EventStore.prototype.get = function( id, version, callback ) {
  var db = this.db
  var tableName = this.tableName

  var params = {
    TableName: tableName
  , KeyConditionExpression: "id = :id and version > :version"
  , ExpressionAttributeValues: { ":id": id, ":version": version }
  }

  db.query( params, function( err, data ) {
    callback( err ? console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

EventStore.prototype.append = function( event, callback ) {
  var db = this.db
  var tableName = this.tableName

  var isValid = ["name","id","version","data"].reduce( function( valid, key ) {
    return valid && ( Object.keys( event ).indexOf( key ) > -1 )
  }, true )

  if ( !isValid ) {
    callback( "invalid event" )
  }
  else {
    var params = {
      TableName: tableName
    , Item: serializeData( event )
    }

    db.put( params, function( err ) {
      callback( err ? console.log( err ) : undefined )
    } )
  }
}

exports.create = function( tableName, endpoint ) {
  return new EventStore( tableName, endpoint )
}
