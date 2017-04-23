var AWS = require( "aws-sdk" )
var test = require( "ava" ).cb

var EventStore = require( "./index.js" )

var DYNAMODB_TABLENAME = "events"
var DYNAMODB_ENDPOINT = "http://0.0.0.0:8000"

var db = new AWS.DynamoDB( {
  region: "local"
, endpoint: DYNAMODB_ENDPOINT
} )

test.before( function( t ) {
  var tableParams = {
    TableName: DYNAMODB_TABLENAME
  , AttributeDefinitions: [
      { AttributeName: "id" , AttributeType: "S" }
    , { AttributeName: "version" , AttributeType: "N" }
    ]
  , KeySchema: [
      { AttributeName: "id", KeyType: "HASH" }
    , { AttributeName: "version", KeyType: "RANGE" }
    ]
  , ProvisionedThroughput: { ReadCapacityUnits: 1 , WriteCapacityUnits: 1 }
  }

  db.deleteTable( { TableName: DYNAMODB_TABLENAME }, function() {
    db.createTable( tableParams, t.end )
  } )
} )

test( "appending an event", function( t ) {
  var es = EventStore.create( DYNAMODB_TABLENAME, DYNAMODB_ENDPOINT )
  var event1 = { version: 1, name: "Created", id: "1", data: { name: "foo" } }
  var event2 = { version: 2, name: "Created", id: "2", data: { name: "bar" } }

  es.append( event1, function() {
    es.append( event2, function() {
      es.get( "1", 0, function( events ) {
        t.deepEqual( events, [
          { id: "1", data: { name: "foo" }, version: 1, name: "Created" }
        ] )
        es.getAll( function( events ) {
          t.is( events.length, 2 )
          t.end()
        } )
      } )
    } )
  } )
} )
