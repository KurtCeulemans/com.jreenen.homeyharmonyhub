'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
const Hub = require('../lib/hub.js');
const { deviceEventKey } = require('../lib/hubidentity.js');

class FakeWebSocket extends EventEmitter {

    constructor(url) {
        super();
        this.url = url;
        this.readyState = FakeWebSocket.CONNECTING;
        this.sent = [];
        FakeWebSocket.instances.push(this);
        if (FakeWebSocket.autoOpen)
            queueMicrotask(() => this.open());
    }

    open() {
        if (this.readyState === FakeWebSocket.OPEN)
            return;
        this.readyState = FakeWebSocket.OPEN;
        this.emit('open');
    }

    send(data, cb) {
        this.sent.push(data);
        if (typeof cb === 'function')
            cb();
    }

    ping() {
        queueMicrotask(() => this.emit('pong'));
    }

    terminate() {
        this.readyState = FakeWebSocket.CLOSED;
        queueMicrotask(() => this.emit('close', 1006, 'terminate'));
    }

}

FakeWebSocket.CONNECTING = 0;
FakeWebSocket.OPEN = 1;
FakeWebSocket.CLOSED = 3;
FakeWebSocket.instances = [];
FakeWebSocket.autoOpen = true;

function resetFakeSockets() {
    FakeWebSocket.instances = [];
    FakeWebSocket.autoOpen = true;
}

function createHarness() {
    const timers = [];
    let nextId = 1;
    const homey = {
        setTimeout(fn, ms) {
            const timer = { id: nextId++, fn, ms, type: 'timeout', cleared: false };
            timers.push(timer);
            return timer.id;
        },
        clearTimeout(id) {
            const timer = timers.find(item => item.id === id);
            if (timer)
                timer.cleared = true;
        },
        setInterval(fn, ms) {
            const timer = { id: nextId++, fn, ms, type: 'interval', cleared: false };
            timers.push(timer);
            return timer.id;
        },
        clearInterval(id) {
            const timer = timers.find(item => item.id === id);
            if (timer)
                timer.cleared = true;
        },
        app: new EventEmitter()
    };
    const parent = new EventEmitter();
    parent.homey = homey;
    return { parent, homey, timers };
}

function fireTimers(timers, ms) {
    timers.filter(timer => !timer.cleared && timer.ms === ms).forEach((timer) => {
        if (timer.type === 'timeout')
            timer.cleared = true;
        timer.fn();
    });
}

async function flush(times) {
    const rounds = times == null ? 8 : times;
    for (let i = 0; i < rounds; i++)
        await Promise.resolve();
}

function lastRequest(ws, predicate) {
    const replied = ws.repliedIds || new Set();
    for (let i = ws.sent.length - 1; i >= 0; i--) {
        if (!ws.sent[i])
            continue;
        const request = JSON.parse(ws.sent[i]);
        if (replied.has(request.hbus.id))
            continue;
        if (!predicate || predicate(request))
            return request;
    }
    return undefined;
}

async function waitForRequest(ws, predicate) {
    for (let i = 0; i < 20; i++) {
        const request = lastRequest(ws, predicate);
        if (request)
            return request;
        await Promise.resolve();
    }
    assert.fail('expected a hub request');
}

function replyTo(hub, ws, request, data) {
    assert.ok(request, 'expected a hub request');
    ws.repliedIds = ws.repliedIds || new Set();
    ws.repliedIds.add(request.hbus.id);
    hub.handleMessage(JSON.stringify({
        cmd: request.hbus.cmd,
        code: 200,
        id: request.hbus.id,
        data
    }));
}

const SAMPLE_ACTIVITIES = [
    { id: '-1', label: 'PowerOff', type: 'PowerOff', fixit: {} },
    { id: 'netflix', label: 'Netflix', type: 'VirtualTelevisionN', fixit: {} },
    { id: 'prime', label: 'Prime', type: 'VirtualTelevisionN', fixit: {} }
];

async function completeSync(hub, ws, activities) {
    const configRequest = await waitForRequest(ws, request => String(request.hbus.cmd).includes('config'));
    replyTo(hub, ws, configRequest, {
        device: [],
        activity: activities || SAMPLE_ACTIVITIES,
        content: {}
    });
    const activityRequest = await waitForRequest(ws, request => String(request.hbus.cmd).includes('getCurrentActivity'));
    replyTo(hub, ws, activityRequest, { result: '-1' });
    await flush();
}

function fireTimerById(timers, timerId) {
    const timer = timers.find(item => item.id === timerId && !item.cleared);
    assert.ok(timer, 'expected a pending timer');
    if (timer.type === 'timeout')
        timer.cleared = true;
    timer.fn();
}

function createHub(parent, options) {
    return new Hub(parent, {
        ip: '127.0.0.1',
        remoteId: 'hub-1',
        friendlyName: 'Test Hub'
    }, Object.assign({
        WebSocket: FakeWebSocket,
        wsPort: 8088
    }, options || {}));
}

describe('Harmony hub stability', () => {
    let harness;
    let hub;

    beforeEach(() => {
        resetFakeSockets();
        harness = createHarness();
    });

    afterEach(() => {
        if (hub)
            hub.destroy();
        hub = undefined;
    });

    it('times out getAvailableCommands when the hub does not answer', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        hub.websocket();
        await Promise.resolve();
        const pending = hub.getAvailableCommands();
        fireTimers(harness.timers, Hub.REQUEST_TIMEOUT_MS);
        await assert.rejects(pending, /timed out/);
    });

    it('rejects a Harmony request and surfaces the error', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        FakeWebSocket.autoOpen = true;
        hub.websocket();
        await Promise.resolve();
        const ws = FakeWebSocket.instances[0];
        const pending = hub.getAvailableCommands();
        const request = lastRequest(ws, req => String(req.hbus.cmd).includes('config'));
        hub.emit(request.hbus.id, new Error('hub rejected'));
        await assert.rejects(pending, /hub rejected/);
    });

    it('does not run overlapping syncHub work', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        let active = 0;
        let maxActive = 0;
        const original = hub._runSync.bind(hub);
        hub._runSync = async (generation) => {
            active += 1;
            maxActive = Math.max(maxActive, active);
            try {
                return await original(generation);
            } finally {
                active -= 1;
            }
        };

        FakeWebSocket.autoOpen = true;
        hub.websocket();
        await Promise.resolve();
        const first = hub.syncHub();
        const second = hub.syncHub();
        await Promise.resolve();
        await completeSync(hub, FakeWebSocket.instances[0]);
        await first;
        await second;
        assert.equal(maxActive, 1);
    });

    it('queues exactly one extra sync while a sync is running', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        const started = [];
        const original = hub._runSync.bind(hub);
        hub._runSync = (generation) => {
            started.push(generation);
            return original(generation);
        };

        hub.websocket();
        await Promise.resolve();
        const ws = FakeWebSocket.instances[0];
        hub.syncHub();
        hub.syncHub();
        assert.equal(hub._syncAgain, true);
        await completeSync(hub, ws);
        await Promise.resolve();
        await completeSync(hub, ws);
        await Promise.resolve();
        assert.equal(started.length, 2);
    });

    it('ignores a reply that belongs to an old WebSocket generation', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        hub.websocket();
        await Promise.resolve();
        const firstWs = FakeWebSocket.instances[0];
        const staleRequest = lastRequest(firstWs, request => String(request.hbus.cmd).includes('config'));
        const stalePending = hub.getAvailableCommands();
        hub.websocket();
        await Promise.resolve();
        hub.handleMessage(JSON.stringify({
            cmd: staleRequest.hbus.cmd,
            code: 200,
            id: staleRequest.hbus.id,
            data: { activity: SAMPLE_ACTIVITIES, device: [] }
        }));
        await assert.rejects(stalePending, /Stale hub generation|Hub connection lost/);
        assert.equal(hub.activities.length, 0);
    });

    it('reconnects automatically after the WebSocket closes', async () => {
        hub = createHub(harness.parent);
        await flush();
        const first = FakeWebSocket.instances[0];
        first.terminate();
        await flush();
        fireTimerById(harness.timers, hub.reconnectTimerID);
        await flush();
        assert.equal(FakeWebSocket.instances.length, 2);
        assert.equal(FakeWebSocket.instances[1].readyState, FakeWebSocket.OPEN);
    });

    it('makes Netflix and Prime available again after a successful recovery sync', async () => {
        const online = { netflix: 0, prime: 0 };
        const offline = { netflix: 0, prime: 0 };
        harness.homey.app.on(deviceEventKey('hub-1', 'netflix') + '_online', () => { online.netflix += 1; });
        harness.homey.app.on(deviceEventKey('hub-1', 'prime') + '_online', () => { online.prime += 1; });
        harness.homey.app.on(deviceEventKey('hub-1', 'netflix') + '_offline', () => { offline.netflix += 1; });
        harness.homey.app.on(deviceEventKey('hub-1', 'prime') + '_offline', () => { offline.prime += 1; });

        hub = createHub(harness.parent);
        await flush();
        await completeSync(hub, FakeWebSocket.instances[0]);
        assert.equal(online.netflix, 1);
        assert.equal(online.prime, 1);

        FakeWebSocket.instances[0].terminate();
        await flush();
        fireTimerById(harness.timers, hub.unavailableTimerID);
        assert.equal(offline.netflix, 1);
        assert.equal(offline.prime, 1);

        fireTimerById(harness.timers, hub.reconnectTimerID);
        await flush();
        await completeSync(hub, FakeWebSocket.instances[1]);
        assert.equal(online.netflix, 2);
        assert.equal(online.prime, 2);
        assert.equal(hub.devicesMarkedUnavailable, false);
    });

    it('keeps Netflix and Prime available when a later config omits them', async () => {
        const online = { netflix: 0, prime: 0 };
        harness.homey.app.on(deviceEventKey('hub-1', 'netflix') + '_online', () => { online.netflix += 1; });
        harness.homey.app.on(deviceEventKey('hub-1', 'prime') + '_online', () => { online.prime += 1; });

        hub = createHub(harness.parent);
        await flush();
        await completeSync(hub, FakeWebSocket.instances[0]);
        assert.equal(online.netflix, 1);
        assert.equal(online.prime, 1);

        const sync = hub.syncHub();
        await completeSync(hub, FakeWebSocket.instances[0], [
            { id: '-1', label: 'PowerOff', type: 'PowerOff', fixit: {} }
        ]);
        await sync;
        assert.equal(online.netflix, 2);
        assert.equal(online.prime, 2);
    });

    it('does not leak request listeners across repeated reconnects', async () => {
        hub = createHub(harness.parent);
        await flush();
        await completeSync(hub, FakeWebSocket.instances[0]);
        const baseline = hub.eventNames().filter(name => /^\d+$/.test(String(name))).length;

        for (let i = 0; i < 4; i++) {
            FakeWebSocket.instances[FakeWebSocket.instances.length - 1].terminate();
            await flush();
            fireTimerById(harness.timers, hub.reconnectTimerID);
            await flush();
            await completeSync(hub, FakeWebSocket.instances[FakeWebSocket.instances.length - 1]);
        }

        const leftover = hub.eventNames().filter(name => /^\d+$/.test(String(name))).length;
        assert.equal(leftover, baseline);
        const keepaliveTimers = harness.timers.filter(timer => !timer.cleared && timer.type === 'interval');
        assert.ok(keepaliveTimers.length <= 2);
    });

    it('does not re-run onInit from onAdded', () => {
        const activitySource = fs.readFileSync(path.join(__dirname, '../drivers/harmony_activity_driver/device.js'), 'utf8');
        const deviceSource = fs.readFileSync(path.join(__dirname, '../drivers/harmony_device_driver/device.js'), 'utf8');
        assert.equal(activitySource.includes('this.onInit()'), false);
        assert.equal(deviceSource.includes('this.onInit()'), false);
    });

    it('keeps activity status updates working after a recovered sync', async () => {
        const changed = [];
        harness.parent.on('activityChanged', (label) => changed.push(label));
        hub = createHub(harness.parent);
        await flush();
        await completeSync(hub, FakeWebSocket.instances[0]);
        hub.handleMessage(JSON.stringify({
            type: 'connect.stateDigest?notify',
            data: {
                activityId: 'netflix',
                runningActivityList: 'netflix',
                activityStatus: 2,
                syncStatus: 0
            }
        }));
        assert.deepEqual(changed, ['Netflix']);
    });

    it('retries a failed sync while the socket stays open', async () => {
        hub = createHub(harness.parent, { autoConnect: false });
        hub.websocket();
        await flush();
        const ws = FakeWebSocket.instances[0];
        fireTimers(harness.timers, Hub.REQUEST_TIMEOUT_MS);
        await flush();
        assert.equal(hub.activities.length, 0);
        fireTimerById(harness.timers, hub._syncRetryTimer);
        await flush();
        await completeSync(hub, ws);
        assert.equal(hub.activities.some(activity => activity.label === 'Netflix'), true);
    });
});
