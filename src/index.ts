// docs: https://optimalbits.github.io/bull/
import Queue from "bull";
import * as faker from "faker";
import chalk from "chalk";
import _ from "lodash";

// the payload for a simulated task
interface IPayload {
    email: string;
    username: string;
    settings: object;
}

// each job can have a name, e.g., process an `image` or a `video`
enum JobType {
    A = "image",
    B = "video",
}

const flipCoin = (): boolean => _.sample([true, false]);

// create the queue
const updateSubscriptionsQueue = new Queue("updateSubscriptions", {
    redis: {
        host: "localhost",
        port: 6379,
    },
    limiter: {
        max: 1, // max one job per
        duration: 1000, // 1 second
    },
});

// create jobs for 10 users
const data = createUserData(10);
const jobs = data.map(async (d) => {
    const jobName = _.sample(JobType);
    return await updateSubscriptionsQueue.add(jobName, d, { attempts: 4 });
});
console.log(chalk.yellow(`created ${jobs.length} jobs`));

// a worker/consumer for JobType.A
updateSubscriptionsQueue.process(JobType.A, async (job) => {
    return await sendPayload(JobType.A, job.data);
});

// a worker/consumer for JobType.B
updateSubscriptionsQueue.process(JobType.B, async (job) => {
    return await sendPayload(JobType.B, job.data);
});

// listeners
updateSubscriptionsQueue.on("completed", (job, result) => {
    console.log(
        `job ${chalk.white(job.id)} completed with ${chalk.magenta.bold(
            result
        )}`
    );
});

updateSubscriptionsQueue.on("failed", (job, err) => {
    const triesRemaining = job.opts.attempts - job.attemptsMade;
    if (triesRemaining > 0) {
        console.error(
            chalk.red`job ${job.id} had an error: ${err}`,
            chalk.green`attempts remaining: ${triesRemaining}`
        );
    } else {
        console.error(chalk.bgRed.black.bold`job ${job.id} permanently failed`);
    }
});

// create some random data
function createUserData(quantity = 10) {
    return _.times(quantity, () => ({
        email: faker.internet.email(),
        username: faker.internet.userName(),
        settings: { isPrettyTrue: faker.random.boolean() },
    }));
}

// a function to simulate doing something
function sendPayload(jobType: JobType, payload: IPayload) {
    return new Promise((resolve, reject) => {
        // cause some tasks to fail
        if (flipCoin()) {
            reject(`task of jobType ${jobType} failed`);
        } else {
            resolve(
                `[type:${jobType}] ${JSON.stringify(payload.settings)} for ${
                    payload.username
                } <${payload.email}>`
            );
        }
    });
}
