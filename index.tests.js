var AWS = require( "aws-sdk" )

var EventStore = require( "./index.js" )

var DYNAMODB_TABLENAME = "events"
var DYNAMODB_ENDPOINT = "http://0.0.0.0:8000"

var db = new AWS.DynamoDB( {
  region: "local"
, endpoint: DYNAMODB_ENDPOINT
} )

beforeAll( function( done ) {
  var tableParams = {
    TableName: DYNAMODB_TABLENAME
  , AttributeDefinitions: [
      { AttributeName: "aggregateId" , AttributeType: "S" }
    , { AttributeName: "version" , AttributeType: "N" }
    ]
  , KeySchema: [
      { AttributeName: "aggregateId", KeyType: "HASH" }
    , { AttributeName: "version", KeyType: "RANGE" }
    ]
  , ProvisionedThroughput: { ReadCapacityUnits: 1 , WriteCapacityUnits: 1 }
  }

  db.deleteTable( { TableName: DYNAMODB_TABLENAME }, function() {
    db.createTable( tableParams, done )
  } )
} )

test( "appending an event", function( done ) {

  var dbc = new AWS.DynamoDB.DocumentClient( {
    region: "local"
  , endpoint: DYNAMODB_ENDPOINT
  , params: { TableName: DYNAMODB_TABLENAME }
  } )

  var es = EventStore.create( dbc )
  var event1 = { version: 1, name: "Created", aggregateId: "1", data: { name: "fizz" } }
  var event2 = { version: 2, name: "Created", aggregateId: "2", data: { name: "buzz" } }

  es.append( event1, function() {
    es.append( event2, function() {
      es.get( "1", 0, function( events ) {
        expect( events ).toEqual( [
          { aggregateId: "1", data: { name: "fizz" }, version: 1, name: "Created" }
        ] )
        es.getAll( function( events ) {
          expect( events.length ).toBe( 2 )
          done()
        } )
      } )
    } )
  } )
} )
