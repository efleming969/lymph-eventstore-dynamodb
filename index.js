var AWS = require( "aws-sdk" )

var deserializeData = function( item ) {
  return Object.assign(
    {}, item, { data: JSON.parse( item.data ) } )
}

var serializeData = function( item ) {
  return Object.assign(
    {}, item, { data: JSON.stringify( item.data ) } )
}

var EventStore = function( tableName, endpoint ) {
  console.log( "connecting to", endpoint + "/" + tableName )
  this.db = new AWS.DynamoDB.DocumentClient( {
    params: { TableName: tableName }
  , endpoint: endpoint
  } )
}

EventStore.prototype.getAll = function( callback ) {
  this.db.scan( {}, function( err, data ) {
    callback( err ? console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

EventStore.prototype.get = function( id, version, callback ) {
  var params = {
    KeyConditionExpression: "id = :id and version > :version"
  , ExpressionAttributeValues: { ":id": id, ":version": version }
  }

  this.db.query( params, function( err, data ) {
    callback( err ? console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

var requiredFields = ["name","id","version","data"]

EventStore.prototype.append = function( event, callback ) {
  var isValid = requiredFields.reduce( function( valid, key ) {
    return valid && ( Object.keys( event ).indexOf( key ) > -1 )
  }, true )

  if ( !isValid ) {
    callback( "invalid event" )
  }
  else {
    var params = { Item: serializeData( event ) }

    this.db.put( params, function( err, data ) {
      callback( err ? console.log( err ) : data )
    } )
  }
}

exports.create = function( tableName, endpoint ) {
  return new EventStore( tableName, endpoint )
}
