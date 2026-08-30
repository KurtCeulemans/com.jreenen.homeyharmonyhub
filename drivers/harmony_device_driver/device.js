'use strict';

const Homey = require('homey');
const HubManager = require('../../lib/hubmanager.js');
const { deviceEventKey, sameHubId } = require('../../lib/hubidentity.js');
const hubManager = new HubManager();

class HarmonyDevice extends Homey.Device {

    async onInit() {
        this._deviceData = this.getData();
        this._hubId = this._deviceData.hubId;
        this._harmonyId = this._deviceData.harmonyId;

        if (!this._hubId || !this._harmonyId) {
            await this.setUnavailable(this.homey.__('repair_required'));
            this.log(`Device ${this._deviceData.id} needs to be re-paired after the identifier change.`);
            return;
        }

        this._eventKey = deviceEventKey(this._hubId, this._harmonyId);
        await this.setUnavailable(`Hub ${this.homey.__('offline')}`);

        this._onOnline = (hub) => {
            if (!this._isSameHub(hub))
                return;
            this.hub = this.homey.app.getHub(this._hubId);
            if (!this.hub)
                return;
            this.setAvailable().catch((err) => this.error(err));
        };

        this._onOffline = (hub) => {
            if (!this._isSameHub(hub))
                return;
            this.setUnavailable(`Hub ${this.homey.__('offline')}`).catch((err) => this.error(err));
        };

        this._onDeviceInitialized = (device) => {
            if (this.device && this._onStateChanged)
                this.device.removeListener('stateChanged', this._onStateChanged);

            this.device = device;
            this._onStateChanged = (state) => {
                if (!state || typeof state !== 'object')
                    return;

                if (this.getCapabilities().find(c => c === 'onoff')) {
                    this.setCapabilityValue('onoff', state.Power === 'On').catch((err) => this.error(err));
                    this.triggerOnOffAction(state);
                }
            };
            device.on('stateChanged', this._onStateChanged);
        };

        this.homey.app.on(`${this._eventKey}_online`, this._onOnline);
        this.homey.app.on(`${this._eventKey}_offline`, this._onOffline);
        hubManager.on(`deviceInitialized_${this._eventKey}`, this._onDeviceInitialized);

        this._registerCapabilityListeners();
        this.log(`Device (${this._harmonyId}) - ${this._deviceData.label} initializing..`);
    }

    _isSameHub(hub) {
        if (!hub)
            return false;
        return sameHubId(hub.remoteId || hub.hubId || hub.uuid, this._hubId);
    }

    _registerCapabilityListeners() {
        this.getCapabilities().forEach(capability => {
            if (capability === 'onoff')
                this.registerCapabilityListener('onoff', (value) => this.onCapabilityOnoff(value));

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
        this.log('device added');
        const foundHub = this.homey.app.getHub(this._deviceData.hubId);
        this.hub = foundHub;

        if (!foundHub || !foundHub.ip)
            return;

        hubManager.connectToHub(foundHub.ip).then((hub) => {
            if (!hub)
                return;
            hub.syncHub();
            if (!hub.devicesMarkedUnavailable && hub.hubConnection && hub.hubConnection.readyState === 1)
                this.setAvailable().catch((err) => this.error(err));
        }).catch((err) => this.error(err));
    }

    onDeleted() {
        this.log('device deleted');
        if (this._eventKey) {
            this.homey.app.removeListener(`${this._eventKey}_online`, this._onOnline);
            this.homey.app.removeListener(`${this._eventKey}_offline`, this._onOffline);
            hubManager.removeListener(`deviceInitialized_${this._eventKey}`, this._onDeviceInitialized);
        }
        if (this.device && this._onStateChanged)
            this.device.removeListener('stateChanged', this._onStateChanged);
    }

    triggerOnOffAction(deviceState) {
        if (!deviceState || typeof deviceState !== 'object')
            return;

        const currenOnOffState = this.getCapabilityValue('onoff');
        const turnedOnDeviceTrigger = this.homey.flow.getDeviceTriggerCard('turned_on');
        const turnedOffDeviceTrigger = this.homey.flow.getDeviceTriggerCard('turned_off');
        const device = this;
        const foundHub = this.homey.app.getHub(this._deviceData.hubId);
        const hub = foundHub;

        if (hub !== undefined) {
            const tokens = {
                hub: hub.friendlyName
            };
            const state = {};
            const deviceTurnedOn = deviceState.Power === 'On';

            if (currenOnOffState !== deviceTurnedOn) {

                if (currenOnOffState === false)
                    turnedOnDeviceTrigger.trigger(device, tokens, state).catch((err) => this.error(err));

                else
                    turnedOffDeviceTrigger.trigger(device, tokens, state).catch((err) => this.error(err));

                this.setCapabilityValue('onoff', deviceTurnedOn).catch((err) => this.error(err));
            }
        }
    }

    onCapabilityOnoff(setOnOffState) {
        let powerGroup = this._deviceData.controlGroup.find(x => x.name === 'Power');
        const foundHub = this.hub;

        if (powerGroup === undefined)
            powerGroup = this._deviceData.controlGroup.find(x => x.name === 'Home');

        if (powerGroup === undefined)
            return Promise.reject(new Error('No power commands available'));

        if (!foundHub)
            return Promise.reject(new Error('Hub is not available'));

        const powerToggleFunction = powerGroup.function.find(x => x.name === 'PowerToggle');
        const powerOnFunction = powerGroup.function.find(x => x.name === 'PowerOn');
        const powerOffFunction = powerGroup.function.find(x => x.name === 'PowerOff');
        let powerCommand = '';

        if (setOnOffState)
            powerCommand = powerOnFunction !== undefined ? powerOnFunction : powerToggleFunction;

        else
            powerCommand = powerOffFunction !== undefined ? powerOffFunction : powerToggleFunction;

        const currentOnOffState = this.getCapabilityValue('onoff');
        if (currentOnOffState !== setOnOffState) {
            const deviceState = {};
            deviceState.Power = setOnOffState ? 'On' : 'Off';
            this.triggerOnOffAction(deviceState);
        } else if (powerCommand === powerToggleFunction)
            return Promise.resolve();

        return hubManager.connectToHub(foundHub.ip).then((hub) => {
            if (!hub)
                throw new Error('Hub connection not found');
            return hub.commandAction(powerCommand);
        });
    }

}

module.exports = HarmonyDevice;
