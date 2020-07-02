import Queue from "bull";
import * as faker from "faker";
import chalk from "chalk";
import _ from "lodash";

interface IPayload {
    email: string;
    username: string;
    settings: object;
}

enum JobType {
    A = "A",
    B = "B",
}

function flipCoin(): boolean {
    return Math.floor(Math.random() * 2) > 0;
}

const connection = {
    host: "localhost",
    port: 6379,
};

// the queue
const updateSubscriptionsQueue = new Queue("updateSubscriptions", {
    redis: connection,
    limiter: {
        max: 1000,
        duration: 5000,
    },
});

// create some random data
const data = _.times(5, () => ({
    email: faker.internet.email(),
    username: faker.internet.userName(),
    settings: { isPrettyTrue: faker.random.boolean() },
}));

const options = {
    delay: 2000,
    attempts: 5,
};

// create jobs
const jobs = data.map(async (d) => {
    const jobName = _.sample(JobType);
    return await updateSubscriptionsQueue.add(jobName, d, options);
});
console.log(
    chalk.yellow(`created ${jobs.length} jobs with a delay of ${options.delay}`)
);

function sendPayload(jobType: JobType, payload: IPayload) {
    // cause some tasks to fail
    if (flipCoin()) {
        throw `task of jobType ${jobType} failed`;
    } else {
        return `[type:${jobType}] ${JSON.stringify(payload.settings)} for ${
            payload.username
        } <${payload.email}>`;
    }
}

// a worker/consumer
updateSubscriptionsQueue.process(JobType.A, async (job) => {
    return sendPayload(JobType.A, job.data);
});
updateSubscriptionsQueue.process(JobType.B, async (job) => {
    return sendPayload(JobType.B, job.data);
});

updateSubscriptionsQueue.on("completed", (job, result) => {
    console.log(
        `job ${chalk.white(job.id)} completed with ${chalk.magenta.bold(result)}`
    );
});

updateSubscriptionsQueue.on("failed", (job, err) => {
    console.error(chalk.red`job ${job.id} had an error:`, err);
});
