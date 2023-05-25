const webpush = require('web-push');
const vapidKeys = webpush.generateVAPIDKeys()
console.log(`"privateVapidKey": "${vapidKeys.privateKey}",\n"publicVapidKey": "${vapidKeys.publicKey}"`)