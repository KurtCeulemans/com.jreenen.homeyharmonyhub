const WebSocket = require('ws');
const EventEmitter = require('events');
const HubDevice = require('./hubdevice.js');
const HubActivity = require('./hubactivity.js');
const { normalizeHubInfo, getCanonicalHubId, deviceEventKey, asId } = require('./hubidentity.js');

const KEEPALIVE_INTERVAL_MS = 10000;
const LIVENESS_TIMEOUT_MS = 60000;
const RECONNECT_INTERVAL_MS = 5000;
const UNAVAILABLE_AFTER_MS = 15000;
const TCP_KEEPALIVE_IDLE_MS = 10000;
const INACTIVITY_INTERVAL_MS = 20000;
const REQUEST_TIMEOUT_MS = 30000;
const HOLD_RELEASE_DELAY_MS = 120;
const INACTIVITY_THRESHOLDS = [5, 10, 15, 30, 60, 120, 240, 480];

class Hub extends EventEmitter {

    constructor(parent, oaObject) {
        super();

        const info = normalizeHubInfo(oaObject);
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
        const previous = this.hubConnection;
        if (previous)
            console.log(`hub.js: replacing socket ${this.ip} old readyState=${previous.readyState}`);

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

        console.log(`hub.js: WebSocket connect started ${this.ip}`);
        const ws = new WebSocket('ws://' + this.ip + ':8088/?domain=svcs.myharmony.com&hubId=' + this.remoteId, { perMessageDeflate: false });
        this.hubConnection = ws;

        ws.on('open', () => {
            if (this.hubConnection !== ws)
                return;

            console.log('SOCKET opened', this.ip);
            this.lastSeen = Date.now();
            this.lastKeepaliveOk = Date.now();
            this._enableTcpKeepalive(ws);
            this._clearUnavailableTimer();
            this._startKeepaliveTimers();
            this.syncHub();
        });

        ws.on('message', (data) => {
            if (this.hubConnection !== ws)
                return;

            this.lastSeen = Date.now();
            if (this._isEmptyPayload(data))
                return;

            this.handleMessage(data);
        });

        ws.on('pong', () => {
            if (this.hubConnection !== ws)
                return;

            this.lastSeen = Date.now();
            this.lastKeepaliveOk = Date.now();
        });

        ws.on('close', () => {
            if (this.hubConnection !== ws)
                return;

            console.log('SOCKET closed', this.ip);
            this.hubConnection = null;
            this._onConnectionLost();
        });

        ws.on('error', (data) => {
            if (this.hubConnection !== ws)
                return;

            console.log(`hub.js error: ${data}`);
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
        this._startUnavailableTimer();
        this._scheduleReconnect();
    }

    _scheduleReconnect() {
        if (this.reconnectTimerID)
            return;

        console.log(`hub.js: reconnect scheduled for ${this.friendlyName} in ${RECONNECT_INTERVAL_MS}ms`);
        this.reconnectTimerID = this.parent.homey.setTimeout(() => {
            this.reconnectTimerID = null;
            this.websocket();
        }, RECONNECT_INTERVAL_MS);
    }

    _startUnavailableTimer() {
        if (this.unavailableTimerID || this.devicesMarkedUnavailable)
            return;

        this.unavailableTimerID = this.parent.homey.setTimeout(() => {
            this.unavailableTimerID = null;
            this.devicesMarkedUnavailable = true;
            this._emitDevicesOffline();
        }, UNAVAILABLE_AFTER_MS);
    }

    _clearUnavailableTimer() {
        if (this.unavailableTimerID) {
            this.parent.homey.clearTimeout(this.unavailableTimerID);
            this.unavailableTimerID = null;
        }
        this.devicesMarkedUnavailable = false;
    }

    _emitDevicesOffline() {
        this.devices.forEach((device) => {
            this._emitApp(`${this._deviceEventKey(device.id)}_offline`, this);
        });

        this.activities.forEach((activity) => {
            this._emitApp(`${this._deviceEventKey(activity.id)}_offline`, this);
        });
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
                    return;
                }
                this.lastKeepaliveOk = Date.now();
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
        const lastHealthy = Math.max(this.lastSeen, this.lastKeepaliveOk);
        const silentFor = Date.now() - lastHealthy;
        if (silentFor < LIVENESS_TIMEOUT_MS)
            return;

        console.log(`Hub ${this.friendlyName} liveness timeout after ${Math.round(silentFor / 1000)}s without keepalive or inbound data.`);
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
        if (this._syncInProgress) {
            this._syncAgain = true;
            return Promise.resolve();
        }

        this._syncInProgress = true;
        console.log(`Syncing hub: ${this.friendlyName}........`);

        return this._runSync()
            .then(() => {
                if (!this._syncAgain)
                    return;
                this._syncAgain = false;
                return this._runSync();
            })
            .then(() => {
                console.log(`Sync completed on ${this.friendlyName}`);
            })
            .catch((error) => {
                console.error(`Synchub error: ${error}`);
            })
            .then(() => {
                this._syncInProgress = false;
                if (this._syncAgain) {
                    this._syncAgain = false;
                    this.syncHub();
                }
            });
    }

    _runSync() {
        this._clearChildHubListeners();

        return this.getAvailableCommands().then((commands) => {
            const devices = commands.device || [];
            const activities = commands.activity || [];
            this.content = commands.content;
            this.devices = [];
            this.activities = [];

            devices.forEach((device) => {
                this._emitApp(`${this._deviceEventKey(device.id)}_online`, this);
                this.devices.push(new HubDevice(device, this));
            });

            activities.forEach((activity) => {
                this._emitApp(`${this._deviceEventKey(activity.id)}_online`, this);
                this.activities.push(new HubActivity(activity, this));
            });

            return this.getCurrentActivity();
        }).then((activityId) => {
            const activity = this.activities.find(x => asId(x.id) === asId(activityId));
            if (!activity) {
                console.log(`Current activity on ${this.friendlyName} is unknown (${activityId})`);
                this.currentActivity = undefined;
                return;
            }

            console.log(`Current activity on ${this.friendlyName}: ${activity.label} (${activityId})`);
            this.currentActivity = activity;

            if (!activity.fixit)
                return;

            for (const propertyName in activity.fixit) {
                const deviceState = activity.fixit[propertyName];
                if (!deviceState || typeof deviceState !== 'object')
                    continue;
                this.emit(`deviceStateChanged_${asId(deviceState.id)}`, deviceState);
            }
        });
    }

    sendRequest(command, params, mssgId) {
        if (params == null)
            params = {
                verb: 'get',
                format: 'json'
            }

        const requestId = mssgId != null ? mssgId : this.msgId;

        if (!this.hubConnection || this.hubConnection.readyState !== WebSocket.OPEN) {
            console.log(`Cannot send request, hub ${this.friendlyName} socket not open`);
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
            if (err)
                console.log(err);

        });
    }

    _waitForResponse(requestId, timeoutMs) {
        const timeout = timeoutMs || REQUEST_TIMEOUT_MS;
        return new Promise((resolve, reject) => {
            const timer = this.parent.homey.setTimeout(() => {
                this.removeAllListeners(requestId);
                reject(new Error(`Hub request ${requestId} timed out`));
            }, timeout);

            this.once(requestId, (err, result) => {
                this.parent.homey.clearTimeout(timer);
                if (err)
                    return reject(err);

                resolve(result);
            });
        });
    }

    getCurrentActivity() {
        this.msgId = this.msgId + 1;
        const requestId = this.msgId;
        this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?getCurrentActivity', null, requestId);

        return this._waitForResponse(requestId).then((result) => {
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

        return this._waitForResponse(requestId).then((result) => result.data);
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

        return this._waitForResponse(requestId);
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

        return this._waitForResponse(requestId).then(() => undefined);
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
                console.log(messageObject);
            }
            if (messageObject.id === 'error')
                console.log(`hub.js: ignored Harmony cmd with reserved id from ${this.friendlyName}`);
            else
                this.emit(messageObject.id, err, messageObject);
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

}
module.exports = Hub;
