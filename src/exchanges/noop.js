const Exchange = require('../exchange');
const WebSocket = require('ws');

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
