const Exchange = require('../exchange');
const WebSocket = require('websocket').w3cwebsocket;

class Noop extends Exchange {

	constructor(options) {
		super(options);

    this.id = 'noop';
	}

	connect() {
		return false;
	}

}
		
module.exports = Noop;
