
const amqp = require("amqplib");
require("dotenv").config();

const transporter = require("./mail");

async function startEmailService() {

    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange("event.exchange", "direct", { durable: true });
    await channel.assertQueue("event.email.queue", { durable: true });
    await channel.bindQueue("event.email.queue", "event.exchange", "event_created");

    console.log("Email Service started. Waiting for messages...");

    channel.consume("event.email.queue", async (msg) => {

        if (!msg) return;

        try {
            const { event, emails } = JSON.parse(msg.content.toString());

            if (emails.length > 0) {
                await transporter.sendMail({
                    from: process.env.MAIL_FROM,
                    to: process.env.MAIL_FROM,
                    bcc: emails,
                    subject: `New Event: ${event.title}`,
                    html: `
                        <h2>${event.title}</h2>
                        <p>${event.description}</p>
                        <p>Date: ${event.eventDate}</p>
                        <p>Venue: ${event.venue}</p>
                    `
                });
            }

            console.log("Email sent:", event.title);
            channel.ack(msg);

        } catch (error) {
            console.error("Email Error:", error.message);
            channel.nack(msg, false, true);
        }
    });
}

startEmailService().catch(console.error);
