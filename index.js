const express = require('express')
const app = express()
const AMPQ = require('amqplib/callback_api');
const redis = require("redis");
const { Pool } = require('pg')

const MESSAGE_QUEUE = 'hello';
const PORT = process.env.PORT || 5000

const REDIS_CONF = {
    host: 'redis-13910.c264.ap-south-1-1.ec2.cloud.redislabs.com',
    port: 13910,
    no_ready_check: true,
}

app.get('/person/:id', async (req, res) => {
    let personId = req.params.id;
    if(personId){
        let result = await getPersonById(req.params.id);
        res.status(200).send(result)
    }else{
        res.status(400).send('MISSING PERSON ID')
    }
})

app.listen(PORT, () => {
    console.log(`http://localhost:${PORT}`)
})

//Redis Connection
const redisClient = redis.createClient(REDIS_CONF);

redisClient.on('error', err => {       
    console.log(err.message)
});

redisClient.auth('6XkvmDVxtZz2ChMsKRLLtp4ZYrW3XpwQ', () => {
    console.log('Authenticated')
});

redisClient.on('connect', async () => { 
    console.log('Connected to Redis..'); 
});


//RabbitMq connection      
AMPQ.connect('amqps://umgtghjv:ilO7cGhzGKowX5uKUFRFC7CLy8G1G3nQ@puffin.rmq2.cloudamqp.com/umgtghjv', (ampqError, connection) => {
    if (ampqError) throw ampqError

    connection.createChannel((error1, channel) => {
        if (error1) throw error1

        channel.assertQueue(MESSAGE_QUEUE, {
            durable: false
        });

        console.log("Waiting for messages in %s queue", MESSAGE_QUEUE);

        channel.consume(MESSAGE_QUEUE, async (msg) => {
            let personId =  msg.content.toString();
            console.log("Received %s",personId);

            redisClient.get(personId, (err, value) => {
                if (err) throw err;
                let result = JSON.parse(value);
                if(result && result.id && result.name && result.city) insertIntoPerson(result)
            });

        }, {
            noAck: true
        });
    });
});

/**
 * Returns created database connection pool.
 */
function startPool(){
    const pool = new Pool({
        user: 'gsensydp',
        host: 'chunee.db.elephantsql.com',
        database: 'gsensydp',
        password: 'Y_xGLSYtGr5K53lE1q4XKcoIC4clJKGZ',
        port: 5432,
    })

    return pool
}

function redisDeleteKey(key){
    redisClient.del(key, () => console.log("Key deleted"));
}

/**
 * @param {obj} person Person Object Ex: {id: 1, name: "shivam", city: "noida" }. 
 * As person_id is primary key in person table,so id must be quique
 */
function insertIntoPerson(person){
    let pool = startPool();
    pool.query(
        'INSERT into person (person_id, name, city) VALUES($1, $2, $3) RETURNING person_id', 
        [person.id, person.name, person.city], (err, result) =>{
            if (err) {
                console.log(err);
            } else {
                console.log(`row inserted with person_id: ${result.rows[0].person_id}`)
                pool.end();
                redisDeleteKey(person.id)
            }
    });
}

/**
 * Returns created row in person table.
 *
 * @param {number} personId person_id to get from person table.
 */
function getPersonById(personId){
    return new Promise((resolve, reject) => {
        let pool = startPool();
        pool.query(`select * from person where person_id=${personId}`, (err, result) => {
            if (err) {
                reject(err);
            } else {
                pool.end();
                resolve(result.rows[0])
            }
        });
    });
}