var readline = require('readline');
var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

var username = makeid();
var channels = [];

var kafka = require('kafka-node'),
    HighLevelProducer = kafka.HighLevelProducer,
    HighLevelConsumer = kafka.HighLevelConsumer,
    KeyedMessage = kafka.KeyedMessage,
    producer_client = new kafka.Client(),
    consumer_client = new kafka.Client(),
    producer = new HighLevelProducer(producer_client);

producer.createTopics(['dummy'], true, function (err, data) {});

var consumer = new HighLevelConsumer(
    	consumer_client,
    	[{topic:'dummy'}],
        {
            groupId: username,
        }
    );

console.log("Welcome to Kafka-IRC Chat");
console.log("===============================================");
console.log("Command List:");
console.log("/JOIN <channel name>: Join channel");
console.log("/LEAVE <channel name>: Leave channel");
console.log("/NICK <your nick>: Change your nick name. Note: Everytime you change your nick, you must rejoin your subscribed channel");
console.log("/EXIT: Exit from application");
console.log('');
console.log('To send message:');
console.log("@<channel name>: Send message to a channel");
console.log("To broadcast to all channel you have joined, just type your message and press enter");
console.log("===============================================");
console.log("Welcome "+username);

consumer.on('message', function (message) {
    console.log(message.value);
});
consumer.on('error', function (err) {
    //console.log('error', err);
});
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
})

rl.on('line', function(line){
	//Join Channel
	if (line.indexOf('/JOIN') == 0){
		var channel = line.substr(6);
	    
		if (channels.indexOf(channel) == -1) {
        	producer.createTopics([channel], false, function (err, data) {
			    var payloads = [
		        	{ topic: channel, messages: "@" + channel + " : " + username + " has joined."}
			    ];
			    producer.send(payloads, function (err, data) {});
			    consumer.addTopics([channel], function (err, added) {},true);
			    channels.push(channel);
			});
        } else {
        	console.log("You are already join this channel!");
        }
	//Leave channel
	} else if (line.indexOf('/LEAVE') == 0){
	    var channel = line.substr(7);
	    
	//Change nikcname
	} else if (line.indexOf('/NICK') == 0){
	    
	//Send message to a channel
	} else if (line.indexOf('@') == 0){
	    var i = line.indexOf(' ');
	    var channel = line.substr(1, i - 1);
	    var msg = line.substr(i + 1);
	    
	    if (channels.indexOf(channel) != -1) {
        	var payloads = [
		        { topic: channel, messages: "@" + channel + " " + username + " : " + msg }
		    ];
		    producer.send(payloads, function (err, data) {
		        console.log(data);
		    });
        } else {
        	console.log("You are not member of this channel!");
        }
	//Exit from application
	} else if (line.indexOf('/EXIT') == 0){
		consumer.close();
		producer.close();
		process.exit(0);
	//Broadcast message
	} else {
	    
	}
});
function makeid()
{
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for( var i=0; i < 5; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}