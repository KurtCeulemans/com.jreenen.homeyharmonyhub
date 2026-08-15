const EventEmitter = require('events');
const Hub = require('./hub.js');
const { getCanonicalHubId, normalizeHubInfo } = require('./hubidentity.js');

let instance = null;

class HubManager extends EventEmitter {

    constructor(homey) {
        super();

        if (!instance) {
            instance = this;
            instance.homey = homey;
            instance._hubsById = {};
            instance._idByIp = {};
            instance.setMaxListeners(50);
        } else if (homey && !instance.homey)
            instance.homey = homey;

        return instance;
    }

    addHub(hubInfo) {
        const normalized = normalizeHubInfo(hubInfo);
        const id = getCanonicalHubId(normalized);
        const existing = this._findHub(id) || this._findHub(normalized.ip);

        if (existing) {
            this._applyHubUpdate(existing, normalized);
            existing.ping();
            return Promise.resolve(existing);
        }

        if (!normalized.ip || !normalized.friendlyName || !normalized.remoteId)
            return Promise.resolve(undefined);

        const hubInstance = new Hub(this, normalized);
        this._indexHub(hubInstance);
        return Promise.resolve(hubInstance);
    }

    connectToHub(host) {
        return Promise.resolve(this._findHub(host));
    }

    _findHub(hostOrId) {
        if (hostOrId == null || hostOrId === '')
            return undefined;

        const key = String(hostOrId);
        if (this._hubsById[key])
            return this._hubsById[key];

        const idByIp = this._idByIp[key];
        if (idByIp)
            return this._hubsById[idByIp];

        return undefined;
    }

    _indexHub(hub) {
        const id = getCanonicalHubId(hub);
        if (!id)
            return;

        this._hubsById[id] = hub;
        if (hub.ip)
            this._idByIp[hub.ip] = id;
    }

    _applyHubUpdate(hub, hubInfo) {
        if (hubInfo.friendlyName)
            hub.friendlyName = hubInfo.friendlyName;
        if (hubInfo.remoteId)
            hub.remoteId = hubInfo.remoteId;
        if (hubInfo.hubId)
            hub.hubId = hubInfo.hubId;
        if (hubInfo.uuid)
            hub.uuid = hubInfo.uuid;

        if (hubInfo.ip && hubInfo.ip !== hub.ip)
            this._reindexIp(hub, hubInfo.ip);
    }

    _reindexIp(hub, nextIp) {
        const id = getCanonicalHubId(hub);
        if (hub.ip && this._idByIp[hub.ip] === id)
            delete this._idByIp[hub.ip];

        hub.updateAddress(nextIp);
        if (id)
            this._idByIp[nextIp] = id;
    }

}

module.exports = HubManager
