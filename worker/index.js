const Twitter = require('twitter')
const StatsD = require('node-statsd')
const cs = new StatsD()
const amqp = require('amqplib/callback_api')
const supported_endpoints = ["statuses/user_timeline", "friends/ids"]

var amqpConn = null;
var pubChannel = null;
function start() {
  amqp.connect(process.env.AMQP_CONNECTION_STRING+"?heartbeat=30", async function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    await startPublisher();
    startWorker();
  });
}

async function startPublisher() {
    return new Promise((resolve, reject) => {
        amqpConn.createConfirmChannel(function(err, ch) {
            if (closeOnErr(err)) return;
            ch.on("error", function(err) {
                console.error("[AMQP] channel error", err.message);
                reject()
            });
            ch.on("close", function() {
                console.log("[AMQP] channel closed");
                reject()
            });

            pubChannel = ch;
            resolve();
        });
    })
}

gch = null;
// A worker that acks messages only if processed succesfully
function startWorker() {
  amqpConn.createChannel(function(err, ch) {
    gch = ch;
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });

    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    ch.prefetch(1);
    supported_endpoints.forEach((endpoint) => {
      ch.assertQueue("twitter_jobs:"+endpoint, { durable: true, auto_delete: false }, function(err, _ok) {
        if (closeOnErr(err)) return;
        console.log("consume twitter_jobs:"+endpoint)
        ch.consume("twitter_jobs:"+endpoint, processMsg, { noAck: false });
      });
    });
  });
}

function processMsg(msg) {
  if(!msg) console.error(new Date(), "GREPME", "Canceled by RabbitMQ");
  work(msg, function(ok) {
    try {
      if (ok)
        gch.ack(msg);
      else
        gch.reject(msg, true);
    } catch (e) {
      closeOnErr(e);
    }
  });
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

function getBearerToken() {
    const oauth2 = new (require('oauth').OAuth2)(
        process.env.TWITTER_CONSUMER_KEY,
        process.env.TWITTER_CONSUMER_SECRET,
        'https://api.twitter.com/',
        null,
        'oauth2/token',
        null);
    return new Promise((resolve, reject) => {
        oauth2.getOAuthAccessToken( '', {'grant_type':'client_credentials'}, function (e, access_token, refresh_token, results) {
            if(e) reject(e)
            else resolve(access_token)
        });
    });
}

function gett() {
    const t = {
        init: async () => {
            t.client = new Twitter({
              consumer_key: process.env.TWITTER_CONSUMER_KEY,
              consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
              bearer_token: await getBearerToken()
            });
        },
        get: async (path, params) => {
            if (!t.client) await t.init()
            let tim = process.hrtime();
            const r = await t.client.get(path, params);
            tim = process.hrtime(tim);
            cs.timing("twitter_worker.req", tim[0]*1000 + tim[1]/1000/1000);
            return r;
        },
        post: async (path, params) => {
            if (!t.client) await t.init()
            let tim = process.hrtime();
            const r = await t.client.post(path, params);
            tim = process.hrtime(tim);
            cs.timing("twitter_worker.req", tim[0]*1000 + tim[1]/1000/1000);
            return r;
        }
    };
    return t;
}

const t = gett();

function send_response(cmd, result) {
    console.log(new Date(), "Processed command: ", JSON.stringify(cmd).substr(0, 80))
    const resp = { metadata: cmd.metadata, tag: cmd.tag, result }
    const resp_tag = cmd.reply_to;
    pubChannel.publish("", resp_tag, new Buffer(JSON.stringify(resp)), { persistent: true },
        function(err, ok) {
            if (err) {
                console.error("[AMQP] publish", err);
            }
        });
}

async function work(msg, cb) {
    let cmd = JSON.parse(msg.content.toString());
    let error;
    const result = await t[cmd.method](cmd.path, cmd.params).catch((err) => {
        console.log(new Date(), "Failure!")
        console.log("cmd: ", cmd)
        console.log(err)
        console.log(err.name)
        error = err
    });
    if(result) {
        send_response(cmd, result)
    }
    if(error && error.code) {
        let c = error.code
        if (c == "ECONNRESET" ||
            c == "ENOTFOUND" ||
            c == "EMFILE" ||
            c == "ETIMEDOUT") {
            console.warn(new Date(), "Connection problem.")
            cb(false)
            return
        }
    } else if(error && error.some) {
        if(error.some(e => e.code == 88)) {
            console.warn(new Date(), "Got rate limit error, sleeping for minute...")
            cs.increment("twitter_worker.ratelimited", 1);
            gch.cancel(msg.fields.consumerTag)
            cb(false)
            setTimeout(() => {
                console.log(new Date(), "Restarting consume")
                gch.consume("twitter_jobs:"+cmd.path, processMsg, { noAck: false }, (err, ok) => { if(err) console.error(new Date(), "GREPME", err)});
            }, 60*1000)
            return
        } else if(error.some(e => e.code == 34 || e.code == 50)) {
            send_response(cmd, error[0])
        } else {
            console.log(new Date(), "Unknown error 1")
            cb(false)
            return
        }
    } else if (error && error.name == "Error") {
        send_response(cmd, error)
    }
    cb(true)
}

console.log("Starting with:")
console.log("TWITTER_CONSUMER_KEY="+process.env.TWITTER_CONSUMER_KEY)
console.log("TWITTER_CONSUMER_SECRET="+process.env.TWITTER_CONSUMER_SECRET)
console.log("AMQP_CONNECTION_STRING"+process.env.AMQP_CONNECTION_STRING)
start()
