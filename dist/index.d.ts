type Handler<In, Out> = {
    fn: BulkHandlerFn<In, Out>;
    queue: HandlerQueue<In, Out>;
};
type BulkHandlerFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
type ScalarHandlerFn<In, Out> = (request: In) => Promise<Out>;
type HandlerQueue<In, Out> = {
    key: In;
    resolve: (ret: Out) => void;
}[];
type Scalarize<T extends Record<string, BulkHandlerFn<any, any>>> = {
    [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out> ? ScalarHandlerFn<In, Out> : never;
};
type Handlerize<T extends Record<string, BulkHandlerFn<any, any>>> = {
    [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out> ? Handler<In, Out> : never;
};
export declare class BulkyPlan<MainIn, MainOut, T extends Record<string, BulkHandlerFn<any, any>>> {
    private processor;
    lastSeenHandler: Handler<any, any>;
    handlerRegistry: Handlerize<T>;
    doneCandidates: number;
    totalCandidates: number;
    constructor(processor: (request: MainIn, use: Scalarize<T>) => Promise<MainOut>, registry: T);
    private registerHandlers;
    maybeExecute<In, Out>(handler: Handler<In, Out>): void;
    run(requests: MainIn[]): Promise<Map<MainIn, MainOut>>;
}
export {};
