var deserializeData = function( item ) {
  return Object.assign(
    {}, item, { data: JSON.parse( item.data ) } )
}

var serializeData = function( item ) {
  return Object.assign(
    {}, item, { data: JSON.stringify( item.data ) } )
}

var EventStore = function( dbc ) {
  this.dbc = dbc
}

EventStore.prototype.getAll = function( callback ) {
  this.dbc.scan( {}, function( err, data ) {
    callback( err ? console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

EventStore.prototype.get = function( aggregateId, version, callback ) {
  var params = {
    KeyConditionExpression: "aggregateId = :aggregateId and version > :version"
  , ExpressionAttributeValues: {
      ":aggregateId": aggregateId
    , ":version": version
    }
  }

  this.dbc.query( params, function( err, data ) {
    callback( err ? console.log( err ) : data.Items.map( deserializeData ) )
  } )
}

var requiredFields = ["name","aggregateId","version","data"]

EventStore.prototype.append = function( event, callback ) {
  var isValid = requiredFields.reduce( function( valid, key ) {
    return valid && ( Object.keys( event ).indexOf( key ) > -1 )
  }, true )

  if ( !isValid ) {
    callback( "invalid event" )
  }
  else {
    var params = { Item: serializeData( event ) }

    this.dbc.put( params, function( err, data ) {
      callback( err ? console.log( err ) : data )
    } )
  }
}

exports.create = function( dbc ) {
  return new EventStore( dbc )
}
