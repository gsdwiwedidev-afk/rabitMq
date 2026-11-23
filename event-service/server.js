const express = require("express");
const amqp = require("amqplib");
const { randomUUID } = require("crypto");
require("dotenv").config();

const { events, users } = require("./data");
const app = express();
app.use(express.json());

let channel;


async function connectRabbitMQ() {
    const conn = await amqp.connect("amqp://guest:guest@localhost:5672");
    channel = await conn.createConfirmChannel();

    await channel.assertExchange("event.exchange", "direct", { durable: true });
    await channel.assertQueue("event.mail.queue", { durable: true });
    await channel.bindQueue("event.mail.queue", "event.exchange", "event_created");

   


}


app.post("/api/events", async (req, res) => {
    try {
        const { title, description, eventDate, venue } = req.body;
     
        const event = { id: randomUUID(), title, description, eventDate, venue };
        events.push(event);

        const emails = users.map(user => user.email);

        channel.publish(
            "event.exchange",
            "event_created",
            Buffer.from(JSON.stringify({ event, emails })),
            { persistent: true, contentType: "application/json" }
        );

        channel.publish(
            "event.exchange",
            "event_deleted",
            Buffer.from(JSON.stringify({ event, emails })),
            { persistent: true, contentType: "application/json" }
        );

        await channel.waitForConfirms();

        res.status(201).json({
            success: true,
            message: "Event created and notification queued",
            data: event,
            totalRecipients: emails.length
        });

    } catch (error) {
        console.error(error.message);
        res.status(500).json({ message: "Event processing failed" });
    }
});


app.get("/api/events", (req, res) =>
    res.json({ success: true, data: events })
);

async function startServer() {
    try {
        await connectRabbitMQ();
        app.listen(process.env.PORT || 3001, () =>
            console.log("Event Service running on port 3001")
        );
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }
}

startServer();