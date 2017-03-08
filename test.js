var AWS = require( "aws-sdk" )
var Tap = require( "tap" )

var EventStore = require( "./index.js" )

var DYNAMODB_TABLENAME = "events"

Tap.test( "dynamodb event store", function( t ) {

  var db = new AWS.DynamoDB( {
    region: process.env.AWS_REGION
  , endpoint: process.env.AWS_DYNAMODB_ENDPOINT
  } )

  t.beforeEach( function( callback ) {
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
    , ProvisionedThroughput: {
        ReadCapacityUnits: 1
      , WriteCapacityUnits: 1
      }
    }

    db.createTable( tableParams, callback )
  } )

  t.afterEach( function( callback ) {
    db.deleteTable( { TableName: DYNAMODB_TABLENAME }, callback )
  } )

  t.test( "appending events", function( a ) {
    var eventStore = EventStore.create( DYNAMODB_TABLENAME )

    var aggregate = { id: "1" , version: 0 }

    var eventsToAppend = [
      { name: "Created", data: { name: "foo" } }
    , { name: "Deleted", data: {} }
    ]

    eventStore.append( aggregate, eventsToAppend, function() {
      eventStore.get( "1", 0, function( eventsAppended ) {
        a.equal( eventsAppended.length, 2 )
        var firstEvent = eventsAppended[ 0 ]
        a.equal( firstEvent.id, "1" )
        a.deepEqual( firstEvent.data, { name: "foo" } )
        a.equal( firstEvent.version, 0 )
        a.equal( firstEvent.name, "Created" )
        a.ok( firstEvent.created < new Date().getTime() )
        a.end()
      } )
    } )
  } )

  t.end()
} )
