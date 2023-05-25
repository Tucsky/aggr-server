const webpush = require('web-push');
const vapidKeys = webpush.generateVAPIDKeys()
console.log(`privateVapidKey: ${vapidKeys.privateKey},\npublicVapidKey: ${vapidKeys.publicKey}`)