const webpush = require('web-push');
const vapidKeys = webpush.generateVAPIDKeys()
console.log(`privateKey: ${vapidKeys.privateKey}`)
console.log(`publicKey: ${vapidKeys.publicKey}`)