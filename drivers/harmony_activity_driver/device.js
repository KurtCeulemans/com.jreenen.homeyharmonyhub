'use strict';

const Homey = require('homey');
const HubManager = require('../../lib/hubmanager.js');
const { deviceEventKey, sameHubId } = require('../../lib/hubidentity.js');
const hubManager = new HubManager();

class HarmonyActivity extends Homey.Device {

    async onInit() {
        this._deviceData = this.getData();
        this._hubId = this._deviceData.hubId;
        this._harmonyId = this._deviceData.harmonyId;

        if (!this._hubId || !this._harmonyId) {
            await this.setUnavailable(this.homey.__('repair_required'));
            this.log(`Activity ${this._deviceData.id} needs to be re-paired after the identifier change.`);
            return;
        }

        this._eventKey = deviceEventKey(this._hubId, this._harmonyId);
        await this.setUnavailable(`Hub ${this.homey.__('offline')}`);

        this._onOnline = (hub) => {
            if (!this._isSameHub(hub))
                return;
            this.hub = hub;
            this.setAvailable().catch((err) => this.error(err));
        };

        this._onOffline = (hub) => {
            if (!this._isSameHub(hub))
                return;
            this.setUnavailable(`Hub ${this.homey.__('offline')}`).catch((err) => this.error(err));
        };

        this._onActivityChanged = (activityName, hubId) => {
            if (this.hub === undefined)
                return;

            if (!sameHubId(hubId, this.hub.remoteId || this.hub.uuid))
                return;

            if (activityName === this._deviceData.label) {
                this.log(`Turning on ${this._deviceData.label} at ${this.hub.friendlyName}`)
                this.setCapabilityValue('onoff', true).catch((err) => this.error(err));
            } else {
                this.log(`Turning off ${this._deviceData.label} at ${this.hub.friendlyName}`)
                this.setCapabilityValue('onoff', false).catch((err) => this.error(err));
            }
        };

        this.homey.app.on(`${this._eventKey}_online`, this._onOnline);
        this.homey.app.on(`${this._eventKey}_offline`, this._onOffline);
        hubManager.on('activityChanged', this._onActivityChanged);

        this.registerCapabilityListener('onoff', async (turnon) => {
            this.log(`ON/OFF triggered on ${this._deviceData.label}(${this._harmonyId})`);

            if (this.hub === undefined)
                throw new Error('Hub is not available');

            const hub = await hubManager.connectToHub(this.hub.ip);
            if (!hub)
                throw new Error('Hub connection not found');

            if (turnon)
                return hub.startActivity(this._harmonyId);

            return hub.stopActivity();
        });

        this._registerCapabilityListeners();
        this.log(`Activity (${this._harmonyId}) - ${this._deviceData.label} initializing..`);
    }

    _isSameHub(hub) {
        if (!hub)
            return false;
        return sameHubId(hub.remoteId || hub.hubId || hub.uuid, this._hubId);
    }

    _registerCapabilityListeners() {
        this.getCapabilities().forEach(capability => {
            if (capability === 'volume_up')
                this.registerCapabilityListener('volume_up', () => this._runGroupedCommand('Volume', 'VolumeUp', 'Volume up'));

            if (capability === 'volume_down')
                this.registerCapabilityListener('volume_down', () => this._runGroupedCommand('Volume', 'VolumeDown', 'Volume down'));

            if (capability === 'volume_mute')
                this.registerCapabilityListener('volume_mute', () => this._runGroupedCommand('Volume', 'Mute', 'Volume mute'));

            if (capability === 'channel_up')
                this.registerCapabilityListener('channel_up', () => this._runGroupedCommand('Channel', 'ChannelUp', 'Channel up'));

            if (capability === 'channel_down')
                this.registerCapabilityListener('channel_down', () => this._runGroupedCommand('Channel', 'ChannelDown', 'Channel down'));
        });
    }

    async _runGroupedCommand(groupName, functionName, label) {
        this.log(`${label} triggered on ${this._deviceData.label}`);
        if (!this.hub)
            throw new Error('Hub is not available');

        const group = this._deviceData.controlGroup.find(x => x.name === groupName);
        if (!group)
            throw new Error(`Control group ${groupName} not found`);

        const command = group.function.find(x => x.name === functionName);
        if (!command)
            throw new Error(`Command ${functionName} not found`);

        const hub = await hubManager.connectToHub(this.hub.ip);
        if (!hub)
            throw new Error('Hub connection not found');

        return hub.commandAction(command);
    }

    onAdded() {
        this.log('activity added');
        const foundHub = this.homey.app.getHub(this._deviceData.hubId);
        this.hub = foundHub;
        if (!foundHub || !foundHub.ip)
            return;

        hubManager.connectToHub(foundHub.ip).then((hub) => {
            if (!hub)
                return;

            if (hub.currentActivity)
                if (hub.currentActivity.label === this._deviceData.label)
                    this.setCapabilityValue('onoff', true).catch((err) => this.error(err));
                else
                    this.setCapabilityValue('onoff', false).catch((err) => this.error(err));

            if (!hub.devicesMarkedUnavailable && hub.hubConnection && hub.hubConnection.readyState === 1)
                this.setAvailable().catch((err) => this.error(err));

        }).catch((err) => this.error(err));
    }

    onDeleted() {
        this.log('activity deleted');
        if (this._eventKey) {
            this.homey.app.removeListener(`${this._eventKey}_online`, this._onOnline);
            this.homey.app.removeListener(`${this._eventKey}_offline`, this._onOffline);
        }
        if (this._onActivityChanged)
            hubManager.removeListener('activityChanged', this._onActivityChanged);
    }

}

module.exports = HarmonyActivity;
