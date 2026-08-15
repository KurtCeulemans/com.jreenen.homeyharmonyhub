const WebSocket = require('ws');
const EventEmitter = require('events');
const Homey = require('homey');
const hubDevice = require('./hubdevice.js');
const hubActivity = require('./hubactivity.js');

const KEEPALIVE_INTERVAL_MS = 10000;
const LIVENESS_TIMEOUT_MS = 60000;
const RECONNECT_INTERVAL_MS = 5000;
const UNAVAILABLE_AFTER_MS = 15000;
const TCP_KEEPALIVE_IDLE_MS = 10000;
const INACTIVITY_INTERVAL_MS = 20000;

class Hub extends EventEmitter {

    constructor(parent, oaObject) {
        super();

        this.current_fw_version = oaObject.current_fw_version;
        this.ip = oaObject.ip;
        this.hubProfiles = oaObject.hubProfiles;
        this.productId = oaObject.productId;
        this.uuid = oaObject.uuid;
        this.friendlyName = oaObject.friendlyName;
        this.remoteId = oaObject.remoteId;
        this.protocolVersion = oaObject.protocolVersion;
        this.hubId = oaObject.hubId;
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

        this.on('currentActivityChanged', (activity, hubId) => {
            const foundHub = Homey.app.getHub(hubId);

            if (foundHub == undefined)
                return;

            if (hubId !== foundHub.uuid)
                return;

            if (this.currentActivity !== activity) {
                if (activity.id === '-1' && this.currentActivity.label !== undefined)
                    this.parent.emit('activityStopped', this.currentActivity.label, this.uuid);

                this.currentActivity = activity;
                this.lastActivity = Date.now();
            }
        });

        this.websocket();
    }

    ping() {
        this._sendKeepalive();
    }

    websocket() {
        this._cleanupSocket();
        this._stopKeepaliveTimers();

        if (this.reconnectTimerID) {
            this.parent.homey.clearTimeout(this.reconnectTimerID);
            this.reconnectTimerID = null;
        }

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

        this.hubConnection = null;
        ws.removeAllListeners();
        try {
            if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)
                ws.terminate();
        } catch (err) {
            console.log(`hub.js cleanup error: ${err}`);
        }
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
            Homey.app.emit(`${device.id}_offline`, this);
        });

        this.activities.forEach((activity) => {
            Homey.app.emit(`${activity.id}_offline`, this);
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
            this.parent.emit('inactivitytime', minutesInactive, this.uuid, this);
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
            this.removeAllListeners(`deviceStateChanged_${device.id}`);
        });
        this.activities.forEach((activity) => {
            this.removeAllListeners(`activityChangeMessage_${activity.id}`);
        });
    }

    syncHub() {
        console.log(`Syncing hub: ${this.friendlyName}........`);
        this._clearChildHubListeners();
        this.getDevices().then((devices) => {
            this.devices = [];

            devices.forEach((device) => {
                Homey.app.emit(`${device.id}_online`, this);
                this.devices.push(new hubDevice(device, this));
            });
        }).then(() => this.getActivities().then((activities) => {
            this.activities = [];

            activities.forEach((activity) => {
                Homey.app.emit(`${activity.id}_online`, this);
                this.activities.push(new hubActivity(activity, this));
            });
        }).then(() => this.getCurrentActivity().then((activityId) => {
            const activity = this.activities.find(x => x.id === activityId);
            console.log(`Current activity on ${this.friendlyName}: ${activity.label} (${activityId})`);

            this.currentActivity = activity;

            for (const propertyName in activity.fixit) {
                const deviceState = activity.fixit[propertyName];
                this.emit(`deviceStateChanged_${deviceState.id}`, deviceState);
            }
        }).then(() => this.getHubContent().then(content => {
            this.content = content;
        }))).then(() => {
            console.log(`Sync completed on ${this.friendlyName}`);
        }).catch((error) => {
            console.error(`Synchub error: ${error}`);
        }));
    }

    sendRequest(command, params, mssgId) {
        if (params == null)
            params = {
                verb: 'get',
                format: 'json'
            }

        if (!this.hubConnection || this.hubConnection.readyState !== WebSocket.OPEN) {
            console.log(`Cannot send request, hub ${this.friendlyName} socket not open`);
            const requestId = this.msgId;
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
                id: this.msgId,
                params
            }
        }

        this.hubConnection.send(JSON.stringify(payload), (err) => {
            if (err)
                console.log(err);

        });
    }

    getCurrentActivity() {
        return new Promise((resolve, reject) => {
            this.msgId = this.msgId + 1;

            this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?getCurrentActivity', null, this.msgId);

            this.once(this.msgId, (err, result) => {
                if (err) {
                    console.log(err)
                    return reject(err)
                }

                resolve(result.data.result);
            });
        })
    }

    getActivities() {
        return new Promise((resolve, reject) => {
            this.getAvailableCommands().then((commands) => {
                resolve(commands.activity);
            });
        });
    }

    getDevices() {
        return new Promise((resolve, reject) => {
            this.getAvailableCommands().then((commands) => {
                resolve(commands.device);
            });
        });
    }

    getHubContent() {
        return new Promise((resolve, reject) => {
            this.getAvailableCommands().then((commands) => {
                resolve(commands.content);
            });
        });
    }

    getAvailableCommands() {
        return new Promise((resolve, reject) => {
            this.msgId = this.msgId + 1;
            this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?config', null, this.msgId);

            this.once(this.msgId, (err, result) => {
                if (err) {
                    console.log(err)
                    return reject(err)
                }

                resolve(result.data);
            });
        });
    }

    commandAction(command) {
        return new Promise((resolve, reject) => {
            const params = {
                status: 'press',
                timestamp: '0',
                verb: 'render',
                action: command.action
            }
            this.msgId = this.msgId + 1;
            this.sendRequest('vnd.logitech.harmony/vnd.logitech.harmony.engine?holdAction', params, this.msgId);

            this.once(this.msgId, (err, result) => {
                if (err) {
                    console.log(err)
                    return reject(err)
                }

                resolve(result.data);
            });

            this.lastActivity = Date.now();
        });
    }

    startActivity(activityId) {
        return new Promise((resolve, reject) => {
            const params = {
                async: 'true',
                timestamp: 0,
                args: {
                    rule: 'start'
                },
                activityId: String(activityId)
            }
            this.msgId = this.msgId + 1;
            this.sendRequest('harmony.activityengine?runactivity', params, this.msgId);

            this.once(this.msgId, (err, result) => {
                if (err) {
                    console.log(err)
                    return reject(err)
                }

                resolve();
            });
        });
    }

    stopActivity() {
        return new Promise((resolve, reject) => {
            this.startActivity(-1).then(() => {
                resolve();
            }).catch((err) => {
                console.log(err);
                reject(err);
            });
        });
    }

    handleMessage(message) {
        const messageObject = JSON.parse(message);
        let err = null;

        if (messageObject.hasOwnProperty('cmd')) {

            if (messageObject.code != 200) {
                err = 'Invalid status code';
                console.log(messageObject);
            }
            this.emit(messageObject.id, err, messageObject);
        }

        if (messageObject.hasOwnProperty('type'))
            if (messageObject.type === 'connect.stateDigest?notify') {
                if (messageObject.data.syncStatus === 1) {
                    this.hubIsSyncing = true;
                    return;
                }

                if (this.hubIsSyncing && messageObject.data.syncStatus === 0) {
                    this.hubIsSyncing = false;
                    this.syncHub();
                }

                this.emit(`activityChangeMessage_${messageObject.data.activityId}`, messageObject.data, this.uuid)
            }

    }

}
module.exports = Hub;
