class UpdateIssues {
    errors;
    constructor() {
        this.errors = [];
    }
}
export class BulkyPlan {
    processor;
    lastSeenHandler;
    handlerRegistry;
    doneCandidates = 0;
    totalCandidates = 0;
    constructor(processor, registry) {
        this.processor = processor;
        this.handlerRegistry = {};
        this.registerHandlers(registry);
    }
    registerHandlers(handlers) {
        for (const [name, handler] of Object.entries(handlers)) {
            this.handlerRegistry[name] = {
                fn: handler,
                queue: [],
            }; // TODO: fix this
        }
    }
    maybeExecute(handler) {
        if (handler.queue.length + this.doneCandidates !== this.totalCandidates) {
            // not all candidates have reached the next checkpoint
            return;
        }
        // next checkpoint reached, run bulk operation
        const args = handler.queue.map((item) => item.key);
        handler.fn(args).then((result) => {
            for (const item of handler.queue) {
                item.resolve(result.get(item.key));
            }
            handler.queue = [];
        });
    }
    async run(requests) {
        this.totalCandidates = requests.length;
        const processorHandlers = {};
        for (const handlerName of Object.keys(this.handlerRegistry)) {
            const handler = this.handlerRegistry[handlerName];
            processorHandlers[handlerName] = (async (input) => {
                this.lastSeenHandler = handler;
                return new Promise((resolve) => {
                    handler.queue.push({ key: input, resolve });
                    this.maybeExecute(handler);
                });
            }); // TODO: fix this
        }
        const handlers = requests.map(async (request) => {
            const result = await this.processor(request, processorHandlers);
            if (result) {
                this.doneCandidates += 1;
                this.maybeExecute(this.lastSeenHandler);
            }
            return result;
        });
        const results = await Promise.all(handlers);
        const resultsByKey = new Map(results.map((result, index) => [requests[index], result]));
        return resultsByKey;
    }
}
