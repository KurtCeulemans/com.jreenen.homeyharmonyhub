const WebSocket = require('ws');
const EventEmitter = require('events');
const HubDevice = require('./hubdevice.js');
const HubActivity = require('./hubactivity.js');
const { normalizeHubInfo, getCanonicalHubId, deviceEventKey, asId } = require('./hubidentity.js');

const KEEPALIVE_INTERVAL_MS = 10000;
const LIVENESS_TIMEOUT_MS = 60000;
const RECONNECT_INTERVAL_MS = 5000;
const RECONNECT_MAX_MS = 60000;
const UNAVAILABLE_AFTER_MS = 15000;
const TCP_KEEPALIVE_IDLE_MS = 10000;
const INACTIVITY_INTERVAL_MS = 20000;
const REQUEST_TIMEOUT_MS = 30000;
const HOLD_RELEASE_DELAY_MS = 120;
const SYNC_RETRY_INITIAL_MS = 5000;
const SYNC_RETRY_MAX_MS = 60000;
const INACTIVITY_THRESHOLDS = [5, 10, 15, 30, 60, 120, 240, 480];

class Hub extends EventEmitter {

    constructor(parent, oaObject, options) {
        super();

        const info = normalizeHubInfo(oaObject);
        const opts = options || {};
        this._WebSocket = opts.WebSocket || WebSocket;
        this._wsPort = opts.wsPort || 8088;
        this.current_fw_version = info.current_fw_version;
        this.ip = info.ip;
        this.hubProfiles = info.hubProfiles;
        this.productId = info.productId;
        this.uuid = info.uuid;
        this.friendlyName = info.friendlyName;
        this.remoteId = info.remoteId;
        this.protocolVersion = info.protocolVersion;
        this.hubId = info.hubId;
        this.devices = [];
        this.activities = [];
        this.parent = parent;
        this.lastActivity = Date.now();
        this.lastSeen = Date.now();
        this.lastKeepaliveOk = Date.now();
        this.msgId = 0;
        this.keepaliveTimerID = null;
        this.inActivityTimerID = null;
        this.reconnectTimerID = null;
        this.unavailableTimerID = null;
        this.hubConnection = null;
        this.hubIsSyncing = false;
        this.devicesMarkedUnavailable = false;
        this._syncInProgress = false;
        this._syncAgain = false;
        this._firedInactiveThresholds = new Set();
        this._connectionGeneration = 0;
        this._reconnectAttempt = 0;
        this._syncRetryTimer = null;
        this._syncRetryDelay = SYNC_RETRY_INITIAL_MS;
        this._destroyed = false;

        this.on('currentActivityChanged', (activity, hubId) => {
            if (!this._isOwnHub(hubId))
                return;

            if (this.currentActivity !== activity) {
                if (asId(activity.id) === '-1' && this.currentActivity && this.currentActivity.label !== undefined)
                    this.parent.emit('activityStopped', this.currentActivity.label, this.canonicalId());

                this.currentActivity = activity;
                this._touchActivity();
            }
        });

        if (opts.autoConnect !== false)
            this.websocket();
    }

    canonicalId() {
        return getCanonicalHubId(this);
    }

    _app() {
        return this.parent && this.parent.homey ? this.parent.homey.app : undefined;
    }

    _isOwnHub(hubId) {
        return asId(hubId) === this.canonicalId();
    }

    _deviceEventKey(harmonyId) {
        return deviceEventKey(this.canonicalId(), harmonyId);
    }

    _emitApp(eventName, payload) {
        const app = this._app();
        if (app)
            app.emit(eventName, payload);
    }

    _touchActivity() {
        this.lastActivity = Date.now();
        this._firedInactiveThresholds.clear();
    }

    _assertCurrentGeneration(generation) {
        if (generation !== this._connectionGeneration)
            throw new Error('Stale hub generation');
    }

    updateAddress(ip) {
        const nextIp = typeof ip === 'string' ? ip.trim() : ip;
        if (!nextIp || nextIp === this.ip)
            return;

        console.log(`hub.js: updateAddress ${this.ip} → ${nextIp}`);
        this.ip = nextIp;
        this.websocket();
    }

    ping() {
        this._sendKeepalive();
    }

    websocket() {
        if (this._destroyed)
            return;

        const previous = this.hubConnection;
        if (previous)
            console.log(`hub.js: replacing socket ${this.ip} old readyState=${previous.readyState}`);

        this._connectionGeneration += 1;
        const generation = this._connectionGeneration;
        this._clearPendingRequests();
        this._cleanupSocket();
        this._stopKeepaliveTimers();

        if (this.reconnectTimerID) {
            this.parent.homey.clearTimeout(this.reconnectTimerID);
            this.reconnectTimerID = null;
        }

        if (!this.ip || !this.remoteId) {
            console.log(`Cannot open hub socket, missing ip or remoteId for ${this.friendlyName}`);
            this._scheduleReconnect();
            return;
        }

        console.log(`hub.js: WebSocket connect started ${this.ip} gen=${generation}`);
        const ws = new this._WebSocket('ws://' + this.ip + ':' + this._wsPort + '/?domain=svcs.myharmony.com&hubId=' + this.remoteId, { perMessageDeflate: false });
        this.hubConnection = ws;

        ws.on('open', () => {
            if (this.hubConnection !== ws || this._connectionGeneration !== generation)
                return;

            console.log(`SOCKET opened ${this.ip} gen=${generation}`);
            this.lastSeen = Date.now();
            this.lastKeepaliveOk = Date.now();
            this._enableTcpKeepalive(ws);
            this._clearUnavailableTimer();
            this._startKeepaliveTimers();
            this.syncHub();
        });

        ws.on('message', (data) => {
            if (this.hubConnection !== ws || this._connectionGeneration !== generation)
                return;

            this.lastSeen = Date.now();
            if (this._isEmptyPayload(data))
                return;

            this.handleMessage(data);
        });

        ws.on('pong', () => {
            if (this.hubConnection !== ws || this._connectionGeneration !== generation)
                return;

            this.lastSeen = Date.now();
            this.lastKeepaliveOk = Date.now();
        });

        ws.on('close', (code, reason) => {
            if (this.hubConnection !== ws || this._connectionGeneration !== generation)
                return;

            console.log(`SOCKET closed ${this.ip} gen=${generation} code=${code} reason=${reason || ''}`);
            this.hubConnection = null;
            this._onConnectionLost();
        });

        ws.on('error', (data) => {
            if (this.hubConnection !== ws || this._connectionGeneration !== generation)
                return;

            console.log(`hub.js error gen=${generation}: ${data}`);
            try {
                ws.terminate();
            } catch (err) {
                console.log(`hub.js terminate error: ${err}`);
            }
        });
    }

    _isEmptyPayload(data) {
        if (data == null)
            return true;
        if (typeof data === 'string')
            return data.length === 0;
        if (typeof data.length === 'number')
            return data.length === 0;
        return false;
    }

    _enableTcpKeepalive(ws) {
        const socket = ws._socket;
        if (socket && typeof socket.setKeepAlive === 'function')
            socket.setKeepAlive(true, TCP_KEEPALIVE_IDLE_MS);
    }

    _cleanupSocket() {
        const ws = this.hubConnection;
        if (!ws)
            return;

        const readyState = ws.readyState;
        console.log(`hub.js: cleanup ${this.ip} readyState=${readyState}`);
        this.hubConnection = null;

        const onCleanupError = (err) => {
            console.log(`hub.js: cleanup socket error ${err}`);
        };
        ws.on('error', onCleanupError);

        try {
            if (readyState === WebSocket.OPEN || readyState === WebSocket.CONNECTING)
                ws.terminate();
        } catch (err) {
            console.log(`hub.js cleanup error: ${err}`);
        }

        ws.removeAllListeners();
        ws.on('error', onCleanupError);
    }

    _onConnectionLost() {
        this._stopKeepaliveTimers();
        this._clearPendingRequests();
        this._clearSyncRetry();
        this._startUnavailableTimer();
        this._scheduleReconnect();
    }

    _scheduleReconnect() {
        if (this._destroyed || this.reconnectTimerID)
            return;

        const delay = Math.min(RECONNECT_INTERVAL_MS * Math.pow(2, this._reconnectAttempt), RECONNECT_MAX_MS);
        this._reconnectAttempt = Math.min(this._reconnectAttempt + 1, 10);
        console.log(`hub.js: reconnect scheduled for ${this.friendlyName} in ${delay}ms attempt=${this._reconnectAttempt}`);
        this.reconnectTimerID = this.parent.homey.setTimeout(() => {
            this.reconnectTimerID = null;
            this.websocket();
        }, delay);
    }

    _startUnavailableTimer() {
        if (this.unavailableTimerID || this.devicesMarkedUnavailable)
            return;

        this.unavailableTimerID = this.parent.homey.setTimeout(() => {
            this.unavailableTimerID = null;
            this.devicesMarkedUnavailable = true;
            console.log(`hub.js: marking devices unavailable after ${UNAVAILABLE_AFTER_MS}ms without connection (${this.friendlyName})`);
            this._emitDevicesOffline();
        }, UNAVAILABLE_AFTER_MS);
    }

    _clearUnavailableTimer() {
        if (this.unavailableTimerID) {
            this.parent.homey.clearTimeout(this.unavailableTimerID);
            this.unavailableTimerID = null;
        }
    }

    _emitDevicesOffline() {
        this.devices.forEach((device) => {
            this._emitApp(`${this._deviceEventKey(device.id)}_offline`, this);
        });

        this.activities.forEach((activity) => {
            this._emitApp(`${this._deviceEventKey(activity.id)}_offline`, this);
        });
    }

    _emitDevicesOnline(extraDeviceIds, extraActivityIds) {
        const deviceIds = new Set((this.devices || []).map(device => asId(device.id)));
        const activityIds = new Set((this.activities || []).map(activity => asId(activity.id)));

        (extraDeviceIds || []).forEach((id) => {
            if (id)
                deviceIds.add(asId(id));
        });
        (extraActivityIds || []).forEach((id) => {
            if (id)
                activityIds.add(asId(id));
        });

        deviceIds.forEach((id) => {
            this._emitApp(`${this._deviceEventKey(id)}_online`, this);
        });
        activityIds.forEach((id) => {
            this._emitApp(`${this._deviceEventKey(id)}_online`, this);
        });
        console.log(`hub.js: recovery complete ${this.friendlyName} gen=${this._connectionGeneration}`);
    }

    _startKeepaliveTimers() {
        this._stopKeepaliveTimers();

        this.keepaliveTimerID = this.parent.homey.setInterval(() => {
            this._sendKeepalive();
            this._checkLiveness();
        }, KEEPALIVE_INTERVAL_MS);

        this.inActivityTimerID = this.parent.homey.setInterval(() => {
            const minutesInactive = ((Date.now() - this.lastActivity) / 1000) / 60;
            INACTIVITY_THRESHOLDS.forEach((threshold) => {
                if (minutesInactive < threshold || this._firedInactiveThresholds.has(threshold))
                    return;

                this._firedInactiveThresholds.add(threshold);
                this.parent.emit('inactivitytime', threshold, this.canonicalId(), this);
            });
        }, INACTIVITY_INTERVAL_MS);
    }

    _stopKeepaliveTimers() {
        if (this.keepaliveTimerID) {
            this.parent.homey.clearInterval(this.keepaliveTimerID);
            this.keepaliveTimerID = null;
        }
        if (this.inActivityTimerID) {
            this.parent.homey.clearInterval(this.inActivityTimerID);
            this.inActivityTimerID = null;
        }
    }

    _sendKeepalive() {
        const ws = this.hubConnection;
        if (!ws || ws.readyState !== WebSocket.OPEN)
            return;

        try {
            ws.send('', (err) => {
                if (err) {
                    console.log(`Keepalive send failed for ${this.friendlyName}: ${err}`);
                    try {
                        ws.terminate();
                    } catch (terminateErr) {
                        console.log(`hub.js terminate error: ${terminateErr}`);
                    }
                }
            });

            if (typeof ws.ping === 'function')
                ws.ping();
        } catch (err) {
            console.log(`Keepalive error for ${this.friendlyName}: ${err}`);
            try {
                ws.terminate();
            } catch (terminateErr) {
                console.log(`hub.js terminate error: ${terminateErr}`);
            }
        }
    }

    _checkLiveness() {
        const silentFor = Date.now() - this.lastSeen;
        if (silentFor < LIVENESS_TIMEOUT_MS)
            return;

        console.log(`Hub ${this.friendlyName} liveness timeout after ${Math.round(silentFor / 1000)}s without inbound data or pong.`);
        const ws = this.hubConnection;
        if (!ws)
            return;

        try {
            ws.terminate();
        } catch (err) {
            console.log(`hub.js terminate error: ${err}`);
        }
    }

    _clearPendingRequests() {
        this.eventNames().forEach((name) => {
            if (typeof name === 'number' || (typeof name === 'string' && /^\d+$/.test(name))) {
                this.emit(name, new Error('Hub connection lost'));
                this.removeAllListeners(name);
            }
        });
    }

    _clearChildHubListeners() {
        this.devices.forEach((device) => {
            this.removeAllListeners(`deviceStateChanged_${asId(device.id)}`);
        });
        this.activities.forEach((activity) => {
            this.removeAllListeners(`activityChangeMessage_${asId(activity.id)}`);
        });
    }

    syncHub() {
        if (this._destroyed)
            return Promise.resolve();

        if (this._syncInProgress) {
            this._syncAgain = true;
            console.log(`hub.js: sync queued ${this.friendlyName} gen=${this._connectionGeneration}`);
            return Promise.resolve();
        }

        this._syncInProgress = true;
        const generation = this._connectionGeneration;
        console.log(`hub.js: sync start ${this.friendlyName} gen=${generation}`);

        return this._runSync(generation)
            .then(() => {
                if (!this._syncAgain)
                    return;
                this._syncAgain = false;
                console.log(`hub.js: running queued sync ${this.friendlyName} gen=${this._connectionGeneration}`);
                return this._runSync(this._connectionGeneration);
            })
            .then(() => {
                console.log(`hub.js: sync end ${this.friendlyName} gen=${this._connectionGeneration}`);
            })
            .catch((error) => {
                if (error && error.message === 'Stale hub generation') {
                    console.log(`hub.js: ignored stale sync result ${this.friendlyName} gen=${generation} now=${this._connectionGeneration}`);
                    return;
                }
                console.error(`Synchub error: ${error}`);
                this._scheduleSyncRetry();
            })
            .then(() => {
                this._syncInProgress = false;
                if (this._syncAgain) {
                    this._syncAgain = false;
                    return this.syncHub();
                }
            });
    }

    _runSync(generation) {
        const previousDeviceIds = this.devices.map(device => asId(device.id));
        const previousActivityIds = this.activities.map(activity => asId(activity.id));

        return this.getAvailableCommands().then((commands) => {
            this._assertCurrentGeneration(generation);

            const devices = commands && commands.device;
            const activities = commands && commands.activity;
            if (!Array.isArray(activities) || activities.length === 0)
                throw new Error('Hub config missing activities');

            this._clearChildHubListeners();
            this.content = commands.content;
            this.devices = [];
            this.activities = [];

            (Array.isArray(devices) ? devices : []).forEach((device) => {
                this.devices.push(new HubDevice(device, this));
            });

            activities.forEach((activity) => {
                this.activities.push(new HubActivity(activity, this));
            });

            return this.getCurrentActivity();
        }).then((activityId) => {
            this._assertCurrentGeneration(generation);

            const activity = this.activities.find(x => asId(x.id) === asId(activityId));
            if (!activity) {
                console.log(`Current activity on ${this.friendlyName} is unknown (${activityId})`);
                this.currentActivity = undefined;
            } else {
                console.log(`Current activity on ${this.friendlyName}: ${activity.label} (${activityId})`);
                this.currentActivity = activity;

                if (activity.fixit)
                    for (const propertyName in activity.fixit) {
                        const deviceState = activity.fixit[propertyName];
                        if (!deviceState || typeof deviceState !== 'object')
                            continue;
                        this.emit(`deviceStateChanged_${asId(deviceState.id)}`, deviceState);
                    }
            }

            this._emitDevicesOnline(previousDeviceIds, previousActivityIds);
            this.devicesMarkedUnavailable = false;
            this._reconnectAttempt = 0;
            this._syncRetryDelay = SYNC_RETRY_INITIAL_MS;
            this._clearSyncRetry();
        });
    }

    _scheduleSyncRetry() {
        if (this._destroyed || this._syncRetryTimer)
            return;

        const ws = this.hubConnection;
        if (!ws || ws.readyState !== WebSocket.OPEN)
            return;

        const delay = this._syncRetryDelay;
        this._syncRetryDelay = Math.min(delay * 2, SYNC_RETRY_MAX_MS);
        console.log(`hub.js: sync retry scheduled for ${this.friendlyName} in ${delay}ms`);
        this._syncRetryTimer = this.parent.homey.setTimeout(() => {
            this._syncRetryTimer = null;
            this.syncHub();
        }, delay);
    }

    _clearSyncRetry() {
        if (!this._syncRetryTimer)
            return;
        this.parent.homey.clearTimeout(this._syncRetryTimer);
        this._syncRetryTimer = null;
    }

    sendRequest(command, params, mssgId) {
        if (params == null)
            params = {
                verb: 'get',
                format: 'json'
            }

        const requestId = mssgId != null ? mssgId : this.msgId;

        if (!this.hubConnection || this.hubConnection.readyState !== WebSocket.OPEN) {
            console.log(`Cannot send request ${command}, hub ${this.friendlyName} socket not open`);
            this.parent.homey.setTimeout(() => {
                this.emit(requestId, new Error('Hub connection lost'));
            }, 0);
            return;
        }

        const payload = {
            hubId: this.remoteId,
            timeout: 30,
            hbus: {
                cmd: command,
                id: requestId,
                params
            }
        }

        this.hubConnection.send(JSON.stringify(payload), (err) => {
            if (err) {
                console.log(`hub.js: send failed ${command}: ${err}`);
                this.emit(requestId, err);
            }
        });
    }

    _waitForResponse(requestId, timeoutMs, requestName) {
        const timeout = timeoutMs || REQUEST_TIMEOUT_MS;
        const label = requestName || String(requestId);
        const generation = this._connectionGeneration;
        return new Promise((resolve, reject) => {
            const timer = this.parent.homey.setTimeout(() => {
                this.removeAllListeners(requestId);
                console.log(`hub.js: request timeout ${label} gen=${generation}`);
                reject(new Error(`Hub request ${label} timed out`));
            }, timeout);

            this.once(requestId, (err, result) => {
                this.parent.homey.clearTimeout(timer);
                if (generation !== this._connectionGeneration) {
                    console.log(`hub.js: ignored stale request result ${label} gen=${generation} now=${this._connectionGeneration}`);
                    return reject(new Error('Stale hub generation'));
                }
                if (err) {
                    console.log(`hub.js: request rejected ${label}: ${err}`);
                    return reject(err);
                }

                resolve(result);
            });
        });
    }

    getCurrentActivity() {
        this.msgId = this.msgId + 1;
        const requestId = this.msgId;
        this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?getCurrentActivity', null, requestId);

        return this._waitForResponse(requestId, REQUEST_TIMEOUT_MS, 'getCurrentActivity').then((result) => {
            return asId(result.data.result);
        });
    }

    getActivities() {
        return this.getAvailableCommands().then((commands) => commands.activity);
    }

    getDevices() {
        return this.getAvailableCommands().then((commands) => commands.device);
    }

    getHubContent() {
        return this.getAvailableCommands().then((commands) => commands.content);
    }

    getAvailableCommands() {
        this.msgId = this.msgId + 1;
        const requestId = this.msgId;
        this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?config', null, requestId);

        return this._waitForResponse(requestId, REQUEST_TIMEOUT_MS, 'config').then((result) => result.data);
    }

    commandAction(command) {
        return this._sendHoldAction(command, 'press').then(() => {
            return new Promise((resolve) => {
                this.parent.homey.setTimeout(resolve, HOLD_RELEASE_DELAY_MS);
            });
        }).then(() => this._sendHoldAction(command, 'release'));
    }

    _sendHoldAction(command, status) {
        const params = {
            status,
            timestamp: '0',
            verb: 'render',
            action: command.action
        }
        this.msgId = this.msgId + 1;
        const requestId = this.msgId;
        this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?holdAction', params, requestId);
        this._touchActivity();

        return this._waitForResponse(requestId, REQUEST_TIMEOUT_MS, `holdAction:${status}`);
    }

    startActivity(activityId) {
        const params = {
            async: 'true',
            timestamp: 0,
            args: {
                rule: 'start'
            },
            activityId: String(activityId)
        }
        this.msgId = this.msgId + 1;
        const requestId = this.msgId;
        this.sendRequest('harmony.activityengine?runactivity', params, requestId);

        return this._waitForResponse(requestId, REQUEST_TIMEOUT_MS, 'runactivity').then(() => undefined);
    }

    stopActivity() {
        return this.startActivity(-1);
    }

    handleMessage(message) {
        let messageObject;
        try {
            messageObject = JSON.parse(message);
        } catch (err) {
            console.log(`hub.js: ignored non-JSON message from ${this.friendlyName}: ${err}`);
            return;
        }

        if (!messageObject || typeof messageObject !== 'object' || Array.isArray(messageObject)) {
            console.log(`hub.js: ignored malformed Harmony message from ${this.friendlyName}`);
            return;
        }

        let err = null;

        if (Object.prototype.hasOwnProperty.call(messageObject, 'cmd')) {
            if (messageObject.code !== 200) {
                err = 'Invalid status code';
                console.log(`hub.js: Harmony cmd status ${messageObject.code} id=${messageObject.id}`);
            }
            if (messageObject.id === 'error')
                console.log(`hub.js: ignored Harmony cmd with reserved id from ${this.friendlyName}`);
            else {
                const responseId = typeof messageObject.id === 'string' && /^\d+$/.test(messageObject.id)
                    ? Number(messageObject.id)
                    : messageObject.id;
                this.emit(responseId, err, messageObject);
            }
        }

        if (Object.prototype.hasOwnProperty.call(messageObject, 'type'))
            if (messageObject.type === 'connect.stateDigest?notify') {
                const data = messageObject.data;
                if (!data || typeof data !== 'object' || Array.isArray(data)) {
                    console.log(`hub.js: ignored malformed stateDigest from ${this.friendlyName}`);
                    return;
                }

                if (data.syncStatus === 1) {
                    this.hubIsSyncing = true;
                    return;
                }

                if (this.hubIsSyncing && data.syncStatus === 0) {
                    this.hubIsSyncing = false;
                    this.syncHub();
                }

                const activityId = data.activityId != null
                    ? asId(data.activityId)
                    : undefined;
                if (activityId != null)
                    this.emit(`activityChangeMessage_${activityId}`, data, this.canonicalId())
            }

    }

    destroy() {
        this._destroyed = true;
        this._clearSyncRetry();
        if (this.reconnectTimerID) {
            this.parent.homey.clearTimeout(this.reconnectTimerID);
            this.reconnectTimerID = null;
        }
        this._clearUnavailableTimer();
        this._stopKeepaliveTimers();
        this._cleanupSocket();
        this._clearPendingRequests();
        this._clearChildHubListeners();
        console.log(`hub.js: destroyed ${this.friendlyName}`);
    }

}

Hub.REQUEST_TIMEOUT_MS = REQUEST_TIMEOUT_MS;
Hub.UNAVAILABLE_AFTER_MS = UNAVAILABLE_AFTER_MS;
Hub.RECONNECT_INTERVAL_MS = RECONNECT_INTERVAL_MS;
Hub.LIVENESS_TIMEOUT_MS = LIVENESS_TIMEOUT_MS;
Hub.SYNC_RETRY_INITIAL_MS = SYNC_RETRY_INITIAL_MS;

module.exports = Hub;
